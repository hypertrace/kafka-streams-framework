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

  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.64")
  testImplementation("org.junit.jupiter:junit-jupiter:5.9.2")
  testImplementation("org.mockito:mockito-core:5.2.0")
  testImplementation("com.github.ben-manes.caffeine:caffeine:3.1.8")
}

tasks.test {
  useJUnitPlatform()
}
