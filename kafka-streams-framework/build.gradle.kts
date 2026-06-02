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
  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  api(project(":kafka-streams-serdes"))
  api(platform(project(":kafka-bom")))
  api(platform(commonLibs.hypertrace.bom))
  api("org.apache.kafka:kafka-streams")
  api(localLibs.kafka.streams.avro.serde)
  api(commonLibs.hypertrace.grpcutils.client)

  implementation(localLibs.avro)
  implementation(commonLibs.kafka.clients)
  implementation(commonLibs.hypertrace.framework.metrics)
  implementation(commonLibs.hypertrace.framework.service)
  implementation(commonLibs.commons.lang)

  testCompileOnly(commonLibs.lombok)
  testAnnotationProcessor(commonLibs.lombok)
  testImplementation(localLibs.kafka.streams.test.utils)
  testImplementation(commonLibs.junit.jupiter)
  testImplementation(localLibs.junit.pioneer)
  testImplementation(commonLibs.mockito.core)
  testImplementation(localLibs.hamcrest.core)
  testRuntimeOnly(commonLibs.log4j.slf4j2.impl)
}

// Disabling compatibility check for the test avro definitions.
tasks.named<org.hypertrace.gradle.avro.CheckAvroCompatibility>("avroCompatibilityCheck") {
  setAgainstFiles(null)
}

if (project.hasProperty("includeSource")) {
  tasks {
    withType<Jar> {
      from(sourceSets["main"].allSource)
      duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    }
  }
}
