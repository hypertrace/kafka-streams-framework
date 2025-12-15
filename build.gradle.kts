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
  id("org.hypertrace.docker-java-application-plugin") version "0.11.3" apply false
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

  // Replace org.lz4:lz4-java with at.yawk.lz4:lz4-java to handle Sonatype relocation
  // This MUST be in each consuming repo - BOMs cannot enforce this automatically
  configurations.all {
    resolutionStrategy.dependencySubstitution {
      substitute(module("org.lz4:lz4-java"))
        .using(module("at.yawk.lz4:lz4-java:1.10.1"))
        .because("org.lz4:lz4-java has been relocated to at.yawk.lz4:lz4-java to fix CVE-2025-12183")
    }
  }
}

dependencyCheck {
  format = org.owasp.dependencycheck.reporting.ReportGenerator.Format.ALL.toString()
  suppressionFile = "owasp-suppressions.xml"
  scanConfigurations.add("runtimeClasspath")
  failBuildOnCVSS = 3.0F
}