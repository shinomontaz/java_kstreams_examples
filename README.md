# java_kstreams_examples

## install maven:

1. Download Maven Zip File and Extract
2. Add MAVEN_HOME System Variable
3. Add MAVEN_HOME Directory in PATH Variable
4. Verify Maven Installation with `mvn -version`

## install java
1. Download correct Java ( 32-version will be installed by default, unacceptable )
`https://www.oracle.com/java/technologies/downloads/?er=221886#jdk23-windows`

It seems installer from Java site always sets a 32 version.

2. Add JAVA_HOME System Variable
3. Add JAVA_HOME Directory in PATH Variable
4. Add _JAVA_OPTIONS with proper mem limit ( -Xmx1024M )

## create project
`mvn archetype:generate "-DarchetypeGroupId=org.apache.kafka" "-DarchetypeArtifactId=streams-quickstart-java" "-DarchetypeVersion=3.9.0" "-DgroupId=streams.examples" "-DartifactId=streams-quickstart" "-Dversion=0.1" "-Dpackage=myapps"`

In case of Windows we need to quote all params as mentioned.

This will generate official kafka-streams example with 3 cases: Pipe, split and wordcount.

## build and run

`mvn clean package`

`mvn exec:java "-Dexec.mainClass=myapps.MaxInt"`

