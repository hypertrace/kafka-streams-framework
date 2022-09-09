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
  api("org.apache.kafka:kafka-clients:7.2.1-ccs")
  api("org.apache.avro:avro:1.11.1")

  testImplementation("org.junit.jupiter:junit-jupiter:5.8.2")
}

// Disabling compatibility check for the test avro definitions.
tasks.named<org.hypertrace.gradle.avro.CheckAvroCompatibility>("avroCompatibilityCheck") {
  enabled = false
}
