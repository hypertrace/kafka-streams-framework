import org.hypertrace.gradle.publishing.HypertracePublishExtension
import org.hypertrace.gradle.publishing.License

plugins {
  alias(commonLibs.plugins.hypertrace.repository)
  alias(commonLibs.plugins.hypertrace.ciutils)
  alias(commonLibs.plugins.hypertrace.publish) apply false
  alias(commonLibs.plugins.hypertrace.codestyle) apply false
  alias(commonLibs.plugins.hypertrace.java.convention)
  alias(commonLibs.plugins.owasp.dependencycheck)
  alias(localLibs.plugins.hypertrace.avro) apply false
}

subprojects {
  group = "org.hypertrace.core.kafkastreams.framework"
  pluginManager.withPlugin(rootProject.commonLibs.plugins.hypertrace.publish.get().pluginId) {
    configure<HypertracePublishExtension> {
      license.set(License.APACHE_2_0)
    }
  }

  pluginManager.withPlugin("java") {
    apply(plugin = rootProject.commonLibs.plugins.hypertrace.codestyle.get().pluginId)
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
