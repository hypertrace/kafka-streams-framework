package org.hypertrace.core.kafkastreams.framework;

import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.ConfigUtils;

/**
 * Base class for consolidating multiple independent KafkaStreamApps
 */
public abstract class ConsolidatedKafkaStreamsApp extends KafkaStreamsApp {

  static final String SUB_TOPOLOGY_NAMES_CONFIG_KEY = "sub.topology.names";
  static final String ENV_CLUSTER_NAME_KEY = "cluster.name";
  static final String ENV_POD_NAME_KEY = "pod.name";
  static final String ENV_CONTAINER_NAME_KEY = "container.name";

  private Map<String, Pair<String, KafkaStreamsApp>> jobNameToSubTopology = new HashMap<>();

  public ConsolidatedKafkaStreamsApp(
      ConfigClient configClient) {
    super(configClient);
  }

  @Override
  public StreamsBuilder buildTopology(
      Map<String, Object> properties,
      StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> inputStreams) {

    List<String> subTopologiesNames = getSubTopologiesNames(properties);
    for (String subTopologyName : subTopologiesNames) {
      getLogger().info("Building sub topology: {}", subTopologyName);
      streamsBuilder = buildSubTopology(subTopologyName, properties, streamsBuilder, inputStreams);
    }

    return streamsBuilder;
  }

  public void doStop() {
    jobNameToSubTopology.values()
        .forEach(pair -> pair.getRight().doCleanUpForConsolidatedDeployment());
    super.doStop();
  }

  @Override
  public List<String> getInputTopics(Map<String, Object> properties) {
    Set<String> inputTopics = new HashSet<>();
    for (Map.Entry<String, Pair<String, KafkaStreamsApp>> entry : jobNameToSubTopology.entrySet()) {
      List<String> subTopologyInputTopics = entry.getValue().getRight().getInputTopics(properties);
      inputTopics.addAll(subTopologyInputTopics);
    }
    return new ArrayList<>(inputTopics);
  }

  @Override
  public List<String> getOutputTopics(Map<String, Object> properties) {
    Set<String> outputTopics = new HashSet<>();
    for (Map.Entry<String, Pair<String, KafkaStreamsApp>> entry : jobNameToSubTopology.entrySet()) {
      List<String> subTopologyOutputTopics = entry.getValue().getRight()
          .getOutputTopics(properties);
      outputTopics.addAll(subTopologyOutputTopics);
    }
    return new ArrayList<>(outputTopics);
  }

  protected abstract KafkaStreamsApp getSubTopologyInstance(String name);

  private StreamsBuilder buildSubTopology(
      String subTopologyName,
      Map<String, Object> properties,
      StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> inputStreams) {
    // create an instance and retains is reference to be used later in other methods
    KafkaStreamsApp subTopology = getSubTopologyInstance(subTopologyName);
    jobNameToSubTopology.put(subTopologyName, Pair.of(subTopology.getJobConfigKey(), subTopology));

    // need to use its own copy as input/output topics are different
    Config subTopologyJobConfig = getSubJobConfig(subTopologyName);

    Config jobConfig = getJobConfig(properties);
    if (jobConfig.hasPath(subTopology.getJobConfigKey())) {
      Config subTopologyJobOverrideConfig = jobConfig.getConfig(subTopology.getJobConfigKey());
      subTopologyJobConfig =
          subTopologyJobOverrideConfig.withFallback(subTopologyJobConfig).resolve();
    }

    Map<String, Object> flattenSubTopologyConfig =
        subTopology.getStreamsConfig(subTopologyJobConfig);
    flattenSubTopologyConfig.put(subTopology.getJobConfigKey(), subTopologyJobConfig);

    // add specific job properties
    addProperties(properties, flattenSubTopologyConfig);

    // initialize any dependencies
    subTopology.doInitForConsolidatedDeployment(subTopologyJobConfig);

    // build the sub-topology
    streamsBuilder = subTopology.buildTopology(properties, streamsBuilder, inputStreams);

    // retain per job key and its topology
    jobNameToSubTopology.put(subTopologyName, Pair.of(subTopology.getJobConfigKey(), subTopology));
    return streamsBuilder;
  }

  private List<String> getSubTopologiesNames(Map<String, Object> properties) {
    return getJobConfig(properties).getStringList(SUB_TOPOLOGY_NAMES_CONFIG_KEY);
  }

  private Config getSubJobConfig(String jobName) {
    return configClient.getConfig(
        jobName,
        ConfigUtils.getEnvironmentProperty(ENV_CLUSTER_NAME_KEY),
        ConfigUtils.getEnvironmentProperty(ENV_POD_NAME_KEY),
        ConfigUtils.getEnvironmentProperty(ENV_CONTAINER_NAME_KEY));
  }

  private Config getJobConfig(Map<String, Object> properties) {
    return (Config) properties.get(this.getJobConfigKey());
  }

  private void addProperties(Map<String, Object> baseProps, Map<String, Object> props) {
    props.forEach(
        (k, v) -> {
          if (!baseProps.containsKey(k)) {
            baseProps.put(k, v);
          }
        });
  }

}
