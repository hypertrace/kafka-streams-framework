plugins {
  `java-platform`
  id("org.hypertrace.publish-plugin")
}


var confluentVersion = "7.7.0"
var confluentCcsVersion = "$confluentVersion-ccs"
var protobufVersion = "3.25.5"

dependencies {
  constraints {
    api("com.fasterxml.jackson.core:jackson-databind:2.16.1")
    api("org.xerial.snappy:snappy-java:1.1.10.5") {
      because("[https://nvd.nist.gov/vuln/detail/CVE-2023-34455] in 'org.apache.kafka:kafka-clients:*'")
      because("[https://nvd.nist.gov/vuln/detail/CVE-2023-43642]")
    }
    api("com.google.protobuf:protobuf-java-util:$protobufVersion")
    api("com.squareup.okio:okio:3.4.0") {
      because("https://nvd.nist.gov/vuln/detail/CVE-2023-3635 in io.confluent:kafka-protobuf-serializer:7.4.0")
    }
    api("org.apache.commons:commons-compress:1.26.0") {
      because("https://www.tenable.com/cve/CVE-2024-25710")
    }

    api("io.confluent:kafka-streams-avro-serde:$confluentVersion")
    api("io.confluent:kafka-protobuf-serializer:$confluentVersion")
    api("io.confluent:kafka-avro-serializer:$confluentVersion")
    api("io.confluent:kafka-streams-protobuf-serde:$confluentVersion")
    api("org.apache.kafka:kafka-clients:$confluentCcsVersion")
    api("org.apache.kafka:kafka-streams:$confluentCcsVersion")
    api("org.apache.kafka:kafka-streams-test-utils:$confluentCcsVersion")
    api("org.apache.avro:avro:1.12.0")
  }
}
