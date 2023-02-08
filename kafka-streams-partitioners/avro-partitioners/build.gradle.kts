plugins {
  `java-library`
  jacoco
  id("org.hypertrace.avro-plugin")
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.jacoco-report-plugin")
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  annotationProcessor("org.projectlombok:lombok:1.18.24")
  compileOnly("org.projectlombok:lombok:1.18.24")

  implementation("com.google.guava:guava:31.1-jre")
  implementation("org.apache.avro:avro:1.11.1")
  implementation("com.typesafe:config:1.4.2")
  implementation("org.apache.kafka:kafka-clients:7.2.1-ccs")
  implementation("org.apache.kafka:kafka-streams:7.2.1-ccs")
  implementation("org.slf4j:slf4j-api:1.7.36")

  testImplementation("org.junit.jupiter:junit-jupiter:5.8.2")
  testImplementation("org.junit-pioneer:junit-pioneer:1.7.1")
  testImplementation("org.mockito:mockito-core:4.5.1")
  testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.17.2")
}

// Disabling compatibility check for the test avro definitions.
tasks.named<org.hypertrace.gradle.avro.CheckAvroCompatibility>("avroCompatibilityCheck") {
  enabled = false
  setAgainstFiles(null)
}
