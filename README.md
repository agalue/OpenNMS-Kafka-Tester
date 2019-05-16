# kafka-tester

The idea of this project is to produce metrics for the amount of messages per second received on a given topic, for testing purposes.

# Requirements

* Java 8
* Maven 3

# Compilation

Checkout the repository, and then:

```bash
mvn install
```

The generated JAR with dependencies (onejar) contains everything needed to execute the application.

# Usage

Pass the `-h` option to learn the rest of the options:

```bash
java -jar kafka-tester-1.0.0-SNAPSHOT-jar-with-dependencies.jar -h
Usage: kafka-tester [-hV] -b=server [-f=freq] [-g=id] [-t=topic] [-p=k=v[,
                    k=v...]]...
  -b, --bootstrap-servers=server
                             Kafka bootstrap server list.
                             Example: kafka1:9092
  -f, --report-frequency=freq
                             Metrics report frequency in seconds.
                             Default: 30
  -g, --group-id=id          Kafka consumer Group ID.
                             Default: KafkaTester
  -h, --help                 Show this help message and exit.
  -p, --param=k=v[,k=v...]   Optional Kafka Producer parameters as comma separated
                               list of key-value pairs.
                             Example: -e max.request.size=5000000,acks=1
  -t, --topic=topic          Kafka destination topic name.
                             Default: OpenNMS.Alarms
  -V, --version              Print version information and exit.
```

To use it, here is an example:

```bash
java -jar kafka-tester-1.0.0-SNAPSHOT-jar-with-dependencies.jar -b 192.168.0.1:9092 -t opennms_alarms
```

To stop the process, send `Ctrl-C`.