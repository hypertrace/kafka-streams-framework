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
  constraints {
    implementation("com.fasterxml.jackson.core:jackson-databind:2.15.2")

    implementation("org.xerial.snappy:snappy-java:1.1.10.1") {
      because("[https://nvd.nist.gov/vuln/detail/CVE-2023-34455] in 'org.apache.kafka:kafka-clients:*' > 'org.xerial.snappy:snappy-java:1.1.8.2'")
    }
  }

  api("org.apache.kafka:kafka-clients:7.2.1-ccs")
  api("org.apache.avro:avro:1.11.1")

  testImplementation("org.junit.jupiter:junit-jupiter:5.8.2")
}

// Disabling compatibility check for the test avro definitions.
tasks.named<org.hypertrace.gradle.avro.CheckAvroCompatibility>("avroCompatibilityCheck") {
  enabled = false
}
