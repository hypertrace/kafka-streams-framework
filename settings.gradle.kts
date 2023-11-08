rootProject.name = "kafka-streams-framework"

pluginManagement {
  repositories {
    mavenLocal()
    gradlePluginPortal()
    maven("https://hypertrace.jfrog.io/artifactory/maven")
  }
}

plugins {
  id("org.hypertrace.version-settings") version "0.2.0"
}

include(":kafka-streams-framework")
include(":kafka-streams-serdes")
include(":kafka-streams-partitioners:avro-partitioners")
include(":kafka-streams-partitioners:weighted-group-partitioner")
include(":kafka-bom")
include(":kafka-event-listener")
