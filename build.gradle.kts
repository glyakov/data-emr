import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
	java
	id("com.gradleup.shadow") version "8.3.5"
}

group = "com.epam"
version = "0.0.1-SNAPSHOT"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(11)
	}
}

tasks.withType<Jar> {
	manifest {
		attributes(mapOf(
			"Main-Class" to "com.epam.training.data.DataTrainingApplication"
		))
	}
}

tasks.withType<ShadowJar> {
	isZip64 = true
	archiveFileName.set("traffik_report_app_without_deps.jar")
}

repositories {
	mavenCentral()
}

dependencies {
	compileOnly("org.apache.spark:spark-core_2.13:3.5.3")
	compileOnly("org.apache.spark:spark-sql_2.13:3.5.3")
//	compileOnly("org.apache.hadoop:hadoop-aws:3.4.1")
//	compileOnly("org.apache.hadoop:hadoop-common:3.4.1")
}

tasks.withType<Test> {
	useJUnitPlatform()
}
