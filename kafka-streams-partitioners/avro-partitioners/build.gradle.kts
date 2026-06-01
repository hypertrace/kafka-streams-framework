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

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  implementation(commonLibs.guava)
  implementation(localLibs.avro)
  implementation(commonLibs.typesafe.config)
  implementation(commonLibs.kafka.clients)
  implementation("org.apache.kafka:kafka-streams")
  implementation(commonLibs.slf4j2.api)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(localLibs.junit.pioneer)
  testImplementation(localLibs.mockito.core)
  testRuntimeOnly(localLibs.log4j.slf4j.impl)
}

// Disabling compatibility check for the test avro definitions.
tasks.named<org.hypertrace.gradle.avro.CheckAvroCompatibility>("avroCompatibilityCheck") {
  enabled = false
  setAgainstFiles(null)
}
