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
  api("org.apache.kafka:kafka-streams:6.0.1-ccs")
  implementation("org.apache.avro:avro:1.10.2")
  implementation("org.apache.kafka:kafka-clients:6.0.1-ccs")
  testImplementation("org.junit.jupiter:junit-jupiter:5.7.0")
  constraints {
    api("com.fasterxml.jackson.core:jackson-databind:2.12.6") {
      because("Denial of Service (DoS) [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-COMFASTERXMLJACKSONCORE-2326698] in com.fasterxml.jackson.core:jackson-databind@2.12.2\n" +
              "    introduced by org.apache.avro:avro@1.10.2 > com.fasterxml.jackson.core:jackson-databind@2.12.2 and 2 other path(s)")
    }
    implementation("org.apache.commons:commons-compress:1.21") {
      because("Multiple Vulnerabilities [https://nvd.nist.gov/vuln/detail/CVE-2021-35515] [https://nvd.nist.gov/vuln/detail/CVE-2021-35516] [https://nvd.nist.gov/vuln/detail/CVE-2021-35517] [https://nvd.nist.gov/vuln/detail/CVE-2021-36090] in org.apache.commons:commons-compress@1.20")
    }
  }
}

// Disabling compatibility check for the test avro definitions.
tasks.named<org.hypertrace.gradle.avro.CheckAvroCompatibility>("avroCompatibilityCheck") {
  enabled = false
}
