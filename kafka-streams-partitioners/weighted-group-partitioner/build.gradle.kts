plugins {
  `java-library`
  jacoco
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.jacoco-report-plugin")
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  constraints {
    implementation("com.fasterxml.jackson.core:jackson-databind:2.15.2")

    implementation("org.xerial.snappy:snappy-java:1.1.10.1") {
      because("[https://nvd.nist.gov/vuln/detail/CVE-2023-34455] in 'org.apache.kafka:kafka-clients:*' > 'org.xerial.snappy:snappy-java:1.1.8.2'")
    }
  }

  annotationProcessor("org.projectlombok:lombok:1.18.24")
  compileOnly("org.projectlombok:lombok:1.18.24")

  api("com.typesafe:config:1.4.2")
  api("org.apache.kafka:kafka-streams:7.2.1-ccs")
  api("org.hypertrace.core.grpcutils:grpc-client-utils:0.12.0")
  implementation("com.google.guava:guava:32.0.1-jre")
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.12.0")
  implementation("org.hypertrace.config.service:partitioner-config-service-api:0.1.46")
  implementation("org.slf4j:slf4j-api:1.7.36")

  testImplementation("org.junit.jupiter:junit-jupiter:5.8.2")
  testImplementation("org.junit-pioneer:junit-pioneer:1.7.1")
  testImplementation("org.mockito:mockito-core:4.5.1")
  testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.17.2")
  testRuntimeOnly("io.grpc:grpc-netty:1.56.0")
}
