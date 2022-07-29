package org.hypertrace.core.kafkastreams.framework;

import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.List;
import org.hypertrace.core.serviceframework.PlatformService;
import org.hypertrace.core.serviceframework.config.ConfigClient;

public abstract class MultiKafkaStreamInstanceApp extends PlatformService {

  static final String SUB_TOPOLOGY_NAMES_CONFIG_KEY = "sub.topology.names";

  private final List<KafkaStreamsApp> appsList = new ArrayList<>();

  public MultiKafkaStreamInstanceApp(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected void doInit() {
    Config appConfig = getAppConfig();
    List<String> apps = appConfig.getStringList(SUB_TOPOLOGY_NAMES_CONFIG_KEY);
    for (String app : apps) {
      appsList.add(getSubTopologyInstance(app));
    }
    appsList.forEach(KafkaStreamsApp::doInit);
  }

  @Override
  protected void doStart() {
    appsList.forEach(KafkaStreamsApp::doStart);
  }

  @Override
  protected void doStop() {
    appsList.forEach(KafkaStreamsApp::doStop);
  }

  @Override
  public boolean healthCheck() {
    return appsList.stream().allMatch(KafkaStreamsApp::healthCheck);
  }

  protected abstract KafkaStreamsApp getSubTopologyInstance(String subTopologyName);
}
