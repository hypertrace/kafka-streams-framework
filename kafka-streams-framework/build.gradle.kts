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
  annotationProcessor("org.projectlombok:lombok:1.18.24")
  compileOnly("org.projectlombok:lombok:1.18.24")

  api(project(":kafka-streams-serdes"))
  api("com.typesafe:config:1.4.2")
  api("org.apache.kafka:kafka-streams:6.0.1-ccs")
  api("io.confluent:kafka-streams-avro-serde:6.0.1")

  implementation("com.google.guava:guava:31.1-jre")
  implementation("org.apache.avro:avro:1.10.2")
  implementation("org.apache.kafka:kafka-clients:6.0.1-ccs")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.31")
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.31")
  implementation("org.apache.commons:commons-lang3:3.12.0")


  constraints {
    api("org.glassfish.jersey.core:jersey-common:2.34") {
      because("https://snyk.io/vuln/SNYK-JAVA-ORGGLASSFISHJERSEYCORE-1255637")
    }

    implementation("com.fasterxml.jackson.core:jackson-databind:2.13.2.1") {
      because("Denial of Service (DoS) [High Severity]" +
              "[https://snyk.io/vuln/SNYK-JAVA-COMFASTERXMLJACKSONCORE-2421244] " +
              "in com.fasterxml.jackson.core:jackson-databind@2.13.1")
    }
  }

  testImplementation("org.apache.kafka:kafka-streams-test-utils:6.0.1-ccs")
  testImplementation("org.junit.jupiter:junit-jupiter:5.8.2")
  testImplementation("org.junit-pioneer:junit-pioneer:1.7.0")
  testImplementation("org.mockito:mockito-core:4.5.1")
  testImplementation("org.hamcrest:hamcrest-core:2.2")
  testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.17.2")
}

// Disabling compatibility check for the test avro definitions.
tasks.named<org.hypertrace.gradle.avro.CheckAvroCompatibility>("avroCompatibilityCheck") {
  setAgainstFiles(null)
}

