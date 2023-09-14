package org.hypertrace.core.kafkastreams.framework;

import static io.grpc.Deadline.after;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.RETENTION_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.TOPOLOGY_OPTIMIZATION;
import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;
import static org.apache.kafka.streams.StreamsConfig.producerPrefix;
import static org.apache.kafka.streams.StreamsConfig.topicPrefix;

import com.google.common.collect.Streams;
import com.typesafe.config.Config;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.grpc.MetricCollectingClientInterceptor;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.GrpcRegistryConfig;
import org.hypertrace.core.kafkastreams.framework.listeners.LoggingStateListener;
import org.hypertrace.core.kafkastreams.framework.listeners.LoggingStateRestoreListener;
import org.hypertrace.core.kafkastreams.framework.rocksdb.BoundedMemoryConfigSetter;
import org.hypertrace.core.kafkastreams.framework.timestampextractors.UseWallclockTimeOnInvalidTimestamp;
import org.hypertrace.core.kafkastreams.framework.topics.creator.KafkaTopicCreator;
import org.hypertrace.core.kafkastreams.framework.util.ExceptionUtils;
import org.hypertrace.core.kafkastreams.framework.util.InitialDelayConfigProvider;
import org.hypertrace.core.serviceframework.PlatformService;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class KafkaStreamsApp extends PlatformService {

  public static final String CLEANUP_LOCAL_STATE = "cleanup.local.state";
  public static final String PRE_CREATE_TOPICS = "precreate.topics";
  public static final String KAFKA_STREAMS_CONFIG_KEY = "kafka.streams.config";
  private static final String SHUTDOWN_DURATION = "shutdown.duration";
  private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsApp.class);

  private final GrpcChannelRegistry grpcChannelRegistry;

  protected KafkaStreams app;
  private KafkaStreamsMetrics metrics;
  private Duration shutdownDuration;
  private Duration initialDelay;
  private boolean isSleeping;

  // Visible for testing only
  protected Topology topology;
  protected Map<String, Object> streamsConfig;

  public KafkaStreamsApp(ConfigClient configClient) {
    super(configClient);
    this.grpcChannelRegistry =
        new GrpcChannelRegistry(
            GrpcRegistryConfig.builder()
                .defaultInterceptor(
                    new MetricCollectingClientInterceptor(
                        PlatformMetricsRegistry.getMeterRegistry()))
                .build());
  }

  protected GrpcChannelRegistry getGrpcChannelRegistry() {
    return grpcChannelRegistry;
  }

  @Override
  protected void doInit() {
    try {
      // configure properties
      streamsConfig = mergeProperties(getBaseStreamsConfig(), getJobStreamsConfig(getAppConfig()));
      // build topologies
      Map<String, KStream<?, ?>> sourceStreams = new HashMap<>();
      StreamsBuilder streamsBuilder = new StreamsBuilder();
      streamsBuilder = buildTopology(streamsConfig, streamsBuilder, sourceStreams);
      this.topology = streamsBuilder.build();

      getLogger().info("Finalized kafka streams configuration: {}", streamsConfig);

      // pre-create input/output topics required for kstream application
      preCreateTopics(streamsConfig);

      Properties streamsConfigProps = new Properties();
      streamsConfigProps.putAll(streamsConfig);

      // create kstream app
      app = new KafkaStreams(topology, streamsConfigProps);

      // export kafka streams metrics
      metrics =
          new KafkaStreamsMetrics(
              app,
              Tags.of(
                  Tag.of("kstreams.app", streamsConfigProps.getProperty(APPLICATION_ID_CONFIG))));
      metrics.bindTo(PlatformMetricsRegistry.getMeterRegistry());

      // useful for resetting local state - during testing or any other scenarios where
      // state (rocksdb) needs to be reset
      if (streamsConfig.containsKey(CLEANUP_LOCAL_STATE)) {
        boolean cleanup = Boolean.parseBoolean((String) streamsConfig.get(CLEANUP_LOCAL_STATE));
        if (cleanup) {
          getLogger().info("=== Resetting local state ===");
          app.cleanUp();
        }
      }

      app.setStateListener(new LoggingStateListener(app));
      app.setGlobalStateRestoreListener(new LoggingStateRestoreListener());
      app.setUncaughtExceptionHandler(
          (thread, exception) -> {
            getLogger()
                .error(
                    "Thread=[{}] encountered=[{}]. Will exit.",
                    thread.getName(),
                    ExceptionUtils.getStackTrace(exception));
            System.exit(1);
          });
      this.shutdownDuration = getShutdownDuration();
      this.initialDelay = InitialDelayConfigProvider.getInstance().getInitialDelay(streamsConfig);
      this.isSleeping = false;
      getLogger().info("kafka streams topologies: {}", topology.describe());
    } catch (Exception e) {
      getLogger().error("Error initializing - ", e);
      System.exit(1);
    }
  }

  @Override
  protected void doStart() {
    try {
      delayStartup(initialDelay.toMillis());
      app.start();
    } catch (Exception e) {
      getLogger().error("Error starting - ", e);
      System.exit(1);
    }
  }

  @Override
  protected void doStop() {
    if (metrics != null) {
      metrics.close();
    }
    app.close(shutdownDuration);
    grpcChannelRegistry.shutdown(after(10, SECONDS));
  }

  /**
   * This method is invoked just before a subtopology is created Any dependencies that need to be
   * initialized need to be done here
   *
   * @param subTopologyJobConfig
   */
  protected void doInitForConsolidatedKStreamApp(Config subTopologyJobConfig) {}

  /** Cleanup any dependencies before the {@link ConsolidatedKafkaStreamsApp#doStop()} is invoked */
  protected void doCleanUpForConsolidatedKStreamApp() {}

  @Override
  public boolean healthCheck() {
    return isSleeping || app.state().isRunningOrRebalancing();
  }

  /**
   * @return all common kafka-streams properties. Typically applications don't need to override this
   */
  public Map<String, Object> getBaseStreamsConfig() {
    Map<String, Object> baseStreamsConfig = new HashMap<>();

    // ##########################
    // Streams configurations
    // ##########################
    baseStreamsConfig.put(TOPOLOGY_OPTIMIZATION, "all");
    baseStreamsConfig.put(METRICS_RECORDING_LEVEL_CONFIG, "INFO");
    baseStreamsConfig.put(
        DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, UseWallclockTimeOnInvalidTimestamp.class);
    baseStreamsConfig.put(
        DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        LogAndContinueExceptionHandler.class);
    baseStreamsConfig.put(ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, BoundedMemoryConfigSetter.class);

    // ##########################
    // Default SerDe configurations
    // ##########################
    baseStreamsConfig.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    baseStreamsConfig.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

    // ##########################
    // Producer configurations
    // ##########################
    // Set acks to all for high availability and prevent dataloss
    // default = 1
    baseStreamsConfig.put(producerPrefix(ACKS_CONFIG), "all");
    // Increase linger.ms for better throughput
    // default = 100
    baseStreamsConfig.put(producerPrefix(LINGER_MS_CONFIG), "500");
    // Increase batch.size for better throughput
    // default = 16384
    baseStreamsConfig.put(producerPrefix(BATCH_SIZE_CONFIG), "524288");
    // Enable compression on producer for better throughput
    // default - none
    baseStreamsConfig.put(producerPrefix(COMPRESSION_TYPE_CONFIG), CompressionType.ZSTD.name);

    // ##########################
    // Consumer configurations
    // ##########################
    // default - earliest (kafka streams)
    baseStreamsConfig.put(consumerPrefix(AUTO_OFFSET_RESET_CONFIG), "latest");

    // ##########################
    // Changelog topic configurations
    // ##########################
    baseStreamsConfig.put(topicPrefix(RETENTION_MS_CONFIG), TimeUnit.HOURS.toMillis(12));

    return baseStreamsConfig;
  }

  public abstract StreamsBuilder buildTopology(
      Map<String, Object> streamsConfig,
      StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> sourceStreams);

  public Map<String, Object> getStreamsConfig(Config jobConfig) {
    return new HashMap<>(ConfigUtils.getFlatMapConfig(jobConfig, getStreamsConfigKey()));
  }

  public String getStreamsConfigKey() {
    return KAFKA_STREAMS_CONFIG_KEY;
  }

  public String getJobConfigKey() {
    return String.format("%s-job-config", getServiceName());
  }

  public Logger getLogger() {
    return logger;
  }

  public List<String> getInputTopics(Map<String, Object> properties) {
    return new ArrayList<>();
  }

  public List<String> getOutputTopics(Map<String, Object> properties) {
    return new ArrayList<>();
  }

  private Map<String, Object> getJobStreamsConfig(Config jobConfig) {
    Map<String, Object> properties = getStreamsConfig(jobConfig);
    if (!properties.containsKey(getJobConfigKey())) {
      properties.put(getJobConfigKey(), jobConfig);
    }
    return properties;
  }

  /** Merge the props into baseProps */
  private Map<String, Object> mergeProperties(
      Map<String, Object> baseProps, Map<String, Object> props) {
    baseProps.putAll(props);
    return baseProps;
  }

  private void preCreateTopics(Map<String, Object> properties) {
    Config jobConfig = (Config) properties.get(getJobConfigKey());
    if (jobConfig.hasPath(PRE_CREATE_TOPICS) && jobConfig.getBoolean(PRE_CREATE_TOPICS)) {
      List<String> topics =
          Streams.concat(getInputTopics(properties).stream(), getOutputTopics(properties).stream())
              .collect(Collectors.toList());

      KafkaTopicCreator.createTopics(
          (String) properties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG), topics);
    }
  }

  private Duration getShutdownDuration() {
    Config config = (Config) streamsConfig.get(getJobConfigKey());
    return config.hasPath(SHUTDOWN_DURATION)
        ? config.getDuration(SHUTDOWN_DURATION)
        : Duration.ofSeconds(30);
  }

  private void delayStartup(long initialDelayMillis) {
    if (initialDelayMillis > 0) {
      isSleeping = true;
      try {
        getLogger()
            .info(
                "Sleeping for {} millisecond before kafka streams app is started.",
                initialDelayMillis);
        TimeUnit.MILLISECONDS.sleep(initialDelayMillis);
      } catch (Exception ex) {
        getLogger().error("Error while sleeping before kafka streams app is started. Ignored.", ex);
      }
      isSleeping = false;
    }
  }
}
