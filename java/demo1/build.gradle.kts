plugins {
    java
}

group = "com.whylogs.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("ai.whylabs:whylogs-core:0.0.2b3")
    implementation("org.apache.commons:commons-csv:1.8")
}
