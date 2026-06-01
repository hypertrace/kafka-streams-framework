plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.publish)
  alias(commonLibs.plugins.hypertrace.jacoco)
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  api(platform(project(":kafka-bom")))
  api(platform(commonLibs.hypertrace.bom))
  api("org.apache.kafka:kafka-streams")
  api(commonLibs.hypertrace.grpcutils.client)
  api(commonLibs.typesafe.config)
  implementation(commonLibs.guava)
  implementation(localLibs.hypertrace.grpcutils.context)
  implementation(localLibs.hypertrace.config.partitioner.api)
  implementation(commonLibs.slf4j2.api)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(localLibs.junit.pioneer)
  testImplementation(localLibs.mockito.core)
  testRuntimeOnly(localLibs.log4j.slf4j.impl)
  testRuntimeOnly(localLibs.grpc.netty)
}
