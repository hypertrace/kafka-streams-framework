plugins {
  `java-library`
  jacoco
  id("com.google.protobuf") version "0.9.3"
  id("org.hypertrace.avro-plugin")
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.jacoco-report-plugin")
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  api(platform(project(":kafka-bom")))

  api("org.apache.kafka:kafka-clients")
  api("org.apache.avro:avro")
  api("com.google.protobuf:protobuf-java-util")

  testImplementation("org.junit.jupiter:junit-jupiter:5.8.2")
}

// Disabling compatibility check for the test avro definitions.
tasks.named<org.hypertrace.gradle.avro.CheckAvroCompatibility>("avroCompatibilityCheck") {
  enabled = false
}

sourceSets {
  test {
    java {
      srcDirs("src/test/proto") // Generated Proto classes
    }
  }
}
