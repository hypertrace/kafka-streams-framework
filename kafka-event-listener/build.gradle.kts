plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.publish)
  alias(commonLibs.plugins.hypertrace.jacoco)
  id("java-test-fixtures")
}

dependencies {
  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  api(platform(project(":kafka-bom")))
  api(platform(commonLibs.hypertrace.bom))
  api(commonLibs.kafka.clients)

  implementation(commonLibs.hypertrace.framework.metrics.jakarta)
  testImplementation(commonLibs.junit.jupiter)
  testImplementation(localLibs.mockito.core)
  testImplementation(localLibs.caffeine)

  testFixturesApi(platform(project(":kafka-bom")))
  testFixturesApi(commonLibs.kafka.clients)
}

tasks.test {
  useJUnitPlatform()
}
