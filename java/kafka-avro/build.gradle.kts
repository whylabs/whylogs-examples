plugins {
    java
    application
    id("com.github.davidmc24.gradle.plugin.avro") version "1.0.0"
}

application {
    mainClassName = "com.whylogs.examples.ConsumerDemo"
}

group = "com.whylogs.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven(url = "https://packages.confluent.io/maven/")
}

dependencies {
    implementation("ai.whylabs:whylogs-core:0.0.2b3")
    implementation("org.apache.commons:commons-csv:1.8")
    implementation("org.apache.avro:avro:1.8.2")
    implementation("org.apache.kafka:kafka-clients:2.7.0")
    implementation("io.confluent:kafka-avro-serializer:6.1.0")
    implementation("joda-time:joda-time:2.10.10")
}

// configuration of avro class generation plugin
avro {
    isCreateSetters.set(false)
    isCreateOptionalGetters.set(false)
    isGettersReturnOptional.set(false)
    isOptionalGettersForNullableFieldsOnly.set(false)
    fieldVisibility.set("PUBLIC_DEPRECATED")
    outputCharacterEncoding.set("UTF-8")
    stringType.set("String")
    templateDirectory.set(null as String?)
    isEnableDecimalLogicalType.set(true)
}

task<Exec>("consumer") {
    dependsOn("build")
    // description("Run the whylogs consumer class with ExecTask")
    commandLine( "java", "-classpath", sourceSets["main"].runtimeClasspath.getAsPath(), "com.whylogs.examples.ConsumerDemo")
}


tasks.withType<JavaCompile>().configureEach {
    // warn about deprecated code.
    options.setDeprecation(true)
    //  Sets any additional arguments to be passed to the compiler.
    options.setCompilerArgs(listOf("-Xlint:unchecked"))
}

task<Exec>("producer") {
    dependsOn("build")
    // description("Run the whylogs producer class with ExecTask")
    commandLine( "java", "-classpath", sourceSets["main"].runtimeClasspath.getAsPath(), "com.whylogs.examples.ProducerDemo")
}

