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
  constraints {
    implementation("org.xerial.snappy:snappy-java:1.1.10.1") {
      because("[https://nvd.nist.gov/vuln/detail/CVE-2023-34455] in 'org.apache.kafka:kafka-clients:*' > 'org.xerial.snappy:snappy-java:1.1.8.2'")
    }
  }
  annotationProcessor("org.projectlombok:lombok:1.18.26")
  compileOnly("org.projectlombok:lombok:1.18.26")

  api(project(":kafka-streams-serdes"))
  api("org.apache.kafka:kafka-streams:7.2.1-ccs")
  api("io.confluent:kafka-streams-avro-serde:7.2.1")
  api("org.hypertrace.core.grpcutils:grpc-client-utils:0.12.0")

  implementation("org.apache.avro:avro:1.11.1")
  implementation("org.apache.kafka:kafka-clients:7.2.1-ccs")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.52")
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.52")
  implementation("org.apache.commons:commons-lang3:3.12.0")

  testCompileOnly("org.projectlombok:lombok:1.18.26")
  testAnnotationProcessor("org.projectlombok:lombok:1.18.26")
  testImplementation("org.apache.kafka:kafka-streams-test-utils:7.2.1-ccs")
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
