plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.publish)
  alias(commonLibs.plugins.hypertrace.jacoco)
  alias(localLibs.plugins.hypertrace.avro)
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  api(platform(project(":kafka-bom")))
  api(platform(commonLibs.hypertrace.bom))

  api(commonLibs.kafka.clients)
  api(localLibs.avro)

  testImplementation(commonLibs.junit.jupiter)
}

// Disabling compatibility check for the test avro definitions.
tasks.named<org.hypertrace.gradle.avro.CheckAvroCompatibility>("avroCompatibilityCheck") {
  enabled = false
}
