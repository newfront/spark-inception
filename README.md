# spark-inception
This project is available free of charge as a companion to my Data+AI Summit (2022) talk.

## Setup
> Requirements: Java 11 (jdk), Scala 2.12, SBT (1.5.2)
Export your java 11 location to `~/.zshrc`: (example: `export JAVA11_HOME="/Library/Java/JavaVirtualMachines/openjdk@11.jdk/Contents/Home"`)

## Building
From the root project directory, run the following command.

```bash
sbt clean assembly
docker build . -t mde/spark-inception-controller:1.0.0
```