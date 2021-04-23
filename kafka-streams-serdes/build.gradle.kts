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
        api("com.fasterxml.jackson.core:jackson-databind:2.11.0") {
            because("XML External Entity (XXE) Injection (new) [High Severity][https://snyk.io/vuln/SNYK-JAVA-COMFASTERXMLJACKSONCORE-1048302] in com.fasterxml.jackson.core:jackson-databind@2.10.5\n" +
                "   introduced by com.fasterxml.jackson.core:jackson-databind@2.10.5")
        }
    }
}

// Disabling compatibility check for the test avro definitions.
tasks.named<org.hypertrace.gradle.avro.CheckAvroCompatibility>("avroCompatibilityCheck") {
    enabled = false
}
