import org.hypertrace.gradle.publishing.HypertracePublishExtension
import org.hypertrace.gradle.publishing.License

plugins {
  id("org.hypertrace.repository-plugin") version "0.5.0"
  id("org.hypertrace.ci-utils-plugin") version "0.4.0"
  id("org.hypertrace.avro-plugin") version "0.5.1" apply false
  id("org.hypertrace.publish-plugin") version "1.1.1" apply false
  id("org.hypertrace.jacoco-report-plugin") version "0.3.0" apply false
  id("org.hypertrace.code-style-plugin") version "2.1.2" apply false
  id("org.owasp.dependencycheck") version "12.1.3"
}

subprojects {
  group = "org.hypertrace.core.kafkastreams.framework"
  pluginManager.withPlugin("org.hypertrace.publish-plugin") {
    configure<HypertracePublishExtension> {
      license.set(License.APACHE_2_0)
    }
  }

  pluginManager.withPlugin("java") {
    configure<JavaPluginExtension> {
      sourceCompatibility = JavaVersion.VERSION_11
      targetCompatibility = JavaVersion.VERSION_11

      apply(plugin = "org.hypertrace.code-style-plugin")
    }
  }

  // Handle lz4-java redirect capability conflict:
  // Sonatype added a redirect from org.lz4:lz4-java:1.8.1 -> at.yawk.lz4:lz4-java:1.8.1 to address CVE-2025-12183.
  // Both artifacts declare the same capability, causing a conflict when upgrading from Kafka's org.lz4:lz4-java:1.8.0.
  // This resolution strategy tells Gradle to automatically select the highest version when this conflict occurs.
  configurations.all {
    resolutionStrategy.capabilitiesResolution.withCapability("org.lz4:lz4-java") {
      select("at.yawk.lz4:lz4-java:1.8.1")
      because("Both org.lz4 and at.yawk.lz4 provide lz4-java due to Sonatype redirect")
    }
  }
}

dependencyCheck {
  format = org.owasp.dependencycheck.reporting.ReportGenerator.Format.ALL.toString()
  suppressionFile = "owasp-suppressions.xml"
  scanConfigurations.add("runtimeClasspath")
  failBuildOnCVSS = 3.0F
}