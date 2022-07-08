package org.hypertrace.core.kafkastreams.framework;

import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;

public class ConsolidatedService extends ConsolidatedKafkaStreamsApp{

  public ConsolidatedService(
      ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected KafkaStreamsApp getSubTopologyInstance(String name) {
    switch (name){
      case "sample-kafka-streams-service1":{
        return new Service1(ConfigClientFactory.getClient());
      }
      case "sample-kafka-streams-service2":{
        return new Service2(ConfigClientFactory.getClient());
      }
      default:
        throw new RuntimeException("Unknown sub topology");
    }
  }
}
