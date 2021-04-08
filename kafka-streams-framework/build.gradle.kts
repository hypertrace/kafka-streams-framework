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
    api(project(":kafka-streams-serdes"))
    api("com.typesafe:config:1.4.1")
    api("org.apache.kafka:kafka-streams:6.1.0-ccs")
    api("io.confluent:kafka-streams-avro-serde:6.1.0")

    implementation("com.google.guava:guava:30.1-jre")
    implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.23")
    implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.23")
    implementation("org.apache.kafka:kafka-clients:6.1.0-ccs")

    testImplementation("org.apache.kafka:kafka-streams-test-utils:6.1.0-ccs")
    testImplementation("org.junit.jupiter:junit-jupiter:5.7.0")
    testImplementation("org.junit-pioneer:junit-pioneer:1.1.0")
    testImplementation("org.mockito:mockito-core:3.6.28")
    testImplementation("org.hamcrest:hamcrest-core:2.2")
    testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.14.0")
}

