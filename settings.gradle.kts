rootProject.name = "kafka-streams-framework"

pluginManagement {
  repositories {
    mavenLocal()
    gradlePluginPortal()
    maven("https://us-maven.pkg.dev/hypertrace-repos/maven")
  }
}

plugins {
  id("org.hypertrace.version-settings") version "0.3.0"
}

include(":kafka-streams-framework")
include(":kafka-streams-serdes")
include(":kafka-streams-partitioners:avro-partitioners")
include(":kafka-streams-partitioners:weighted-group-partitioner")
include(":kafka-bom")
include(":kafka-event-listener")
