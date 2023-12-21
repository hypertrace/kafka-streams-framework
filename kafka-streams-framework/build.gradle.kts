plugins {
  `java-library`
  jacoco
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.jacoco-report-plugin")
  id("org.hypertrace.avro-plugin")
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  annotationProcessor("org.projectlombok:lombok:1.18.26")
  compileOnly("org.projectlombok:lombok:1.18.26")

  api(project(":kafka-streams-serdes"))
  api(platform(project(":kafka-bom")))
  api("org.apache.kafka:kafka-streams")
  api("io.confluent:kafka-streams-avro-serde")
  api("org.hypertrace.core.grpcutils:grpc-client-utils:0.13.0")

  implementation("org.apache.avro:avro")
  implementation("org.apache.kafka:kafka-clients")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.64")
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.64")
  implementation("org.apache.commons:commons-lang3:3.12.0")

  testCompileOnly("org.projectlombok:lombok:1.18.26")
  testAnnotationProcessor("org.projectlombok:lombok:1.18.26")
  testImplementation("org.apache.kafka:kafka-streams-test-utils")
  testImplementation("org.junit.jupiter:junit-jupiter:5.9.2")
  testImplementation("org.junit-pioneer:junit-pioneer:2.0.0")
  testImplementation("org.mockito:mockito-core:5.2.0")
  testImplementation("org.hamcrest:hamcrest-core:2.2")
  testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.20.0")
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
