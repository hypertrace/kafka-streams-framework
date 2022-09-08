package org.hypertrace.core.kafkastreams.framework;

import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.List;
import org.hypertrace.core.serviceframework.PlatformService;
import org.hypertrace.core.serviceframework.config.ConfigClient;

public abstract class MultiKafkaStreamsApp extends PlatformService {

  private static final String STREAMS_APPS = "streams.apps";

  private final List<KafkaStreamsApp> appsList = new ArrayList<>();

  public MultiKafkaStreamsApp(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected void doInit() {
    Config appConfig = getAppConfig();
    List<String> apps = appConfig.getStringList(STREAMS_APPS);
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
