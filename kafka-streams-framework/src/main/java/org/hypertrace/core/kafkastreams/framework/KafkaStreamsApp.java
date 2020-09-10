package org.hypertrace.core.kafkastreams.framework;

import com.google.common.collect.Streams;
import com.typesafe.config.Config;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.hypertrace.core.kafkastreams.framework.listeners.LoggingStateListener;
import org.hypertrace.core.kafkastreams.framework.listeners.LoggingStateRestoreListener;
import org.hypertrace.core.kafkastreams.framework.topics.creator.KafkaTopicCreator;
import org.hypertrace.core.kafkastreams.framework.util.ExceptionUtils;
import org.hypertrace.core.serviceframework.PlatformService;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.slf4j.Logger;

public abstract class KafkaStreamsApp extends PlatformService {

  public static final String CLEANUP_LOCAL_STATE = "cleanup.local.state";
  public static final String PRE_CREATE_TOPICS = "precreate.topics";
  protected KafkaStreams app;

  public KafkaStreamsApp(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected void doInit() {
    try {
      Properties streamsConfig = getStreamsConfig(getAppConfig());
      getLogger().info(ConfigUtils.propertiesAsList(streamsConfig));

      // get the lists of all input and output topics to pre create if any
      boolean createTopic = Boolean.parseBoolean(
              (String) streamsConfig.getOrDefault(PRE_CREATE_TOPICS, "false"));
      if (createTopic) {
        List<String> topics = Streams.concat(
                getInputTopics(streamsConfig).stream(),
                getOutputTopics(streamsConfig).stream()
        ).collect(Collectors.toList());

        KafkaTopicCreator.createTopics(streamsConfig.getProperty(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, ""),
                topics);
      }


      Map<String, KStream<?, ?>> sourceStreams = new HashMap<>();
      StreamsBuilder streamsBuilder = new StreamsBuilder();
      streamsBuilder = buildTopology(streamsConfig, streamsBuilder, sourceStreams);
      Topology topology = streamsBuilder.build();
      getLogger().info(topology.describe().toString());

      app = new KafkaStreams(topology, streamsConfig);

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
      app.setUncaughtExceptionHandler((thread, exception) -> {
                getLogger().error("Thread=[{}] encountered=[{}]. Will exit.", thread.getName(),
                        ExceptionUtils.getStackTrace(exception));
                System.exit(1);
              }
      );
    } catch (Exception e) {
      getLogger().error("Error initializing - ", e);
      e.printStackTrace();
      System.exit(1);
    }
  }

  @Override
  protected void doStart() {
    try {
      app.start();
    } catch (Exception e) {
      getLogger().error("Error starting - ", e);
      e.printStackTrace();
      System.exit(1);
    }
  }

  @Override
  protected void doStop() {
    app.close(Duration.ofSeconds(30));
  }

  @Override
  public boolean healthCheck() {
    return true;
  }

  public abstract StreamsBuilder buildTopology(Properties streamsConfig,
                                               StreamsBuilder streamsBuilder, Map<String, KStream<?, ?>> sourceStreams);

  public abstract Properties getStreamsConfig(Config jobConfig);

  public abstract Logger getLogger();

  public abstract List<String> getInputTopics(Properties streamsConfig);

  public abstract List<String> getOutputTopics(Properties streamsConfig);

}
