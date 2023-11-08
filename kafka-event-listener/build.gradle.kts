plugins {
  `java-library`
  jacoco
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  annotationProcessor("org.projectlombok:lombok:1.18.26")
  compileOnly("org.projectlombok:lombok:1.18.26")

  api(platform(project(":kafka-bom")))
  api("org.apache.kafka:kafka-clients")

  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.62")
}
