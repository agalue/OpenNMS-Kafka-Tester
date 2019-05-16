package org.opennms.features.kafka.test;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * The Class KafkaTester.
 * 
 * @author <a href="mailto:agalue@opennms.org">Alejandro Galue</a>
 */
@Command(name = "kafka-tester", mixinStandardHelpOptions = true, version = "1.0.0")
public class KafkaTester implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTester.class);

    @Option(names={"-b","--bootstrap-servers"}, paramLabel="server", description="Kafka bootstrap server list.\nExample: kafka1:9092", required=true)
    String kafkaServers;

    @Option(names={"-t","--topic"}, paramLabel="topic", description="Kafka destination topic name.\nDefault: ${DEFAULT-VALUE}", defaultValue="OpenNMS.Alarms")
    String kafkaTopic;

    @Option(names={"-g","--group-id"}, paramLabel="id", description="Kafka consumer Group ID.\nDefault: ${DEFAULT-VALUE}", defaultValue="KafkaTester")
    String groupId;

    @Option(names={"-p","--param"}, paramLabel="k=v", split=",", description="Optional Kafka Producer parameters as comma separated list of key-value pairs.\nExample: -e max.request.size=5000000,acks=1")
    List<String> kafkaParameters;

    @Option(names={"-f","--report-frequency"}, paramLabel="freq", description="Metrics report frequency in seconds.\nDefault: ${DEFAULT-VALUE}", defaultValue="30")
    Integer frequency;

    private final HashSet<String> uniqueKeys = new HashSet<>();
    private final MetricRegistry metrics = new MetricRegistry();
    private final Meter messagesMeter = metrics.meter("messages");
    private final Counter uniqueKeyMeter = metrics.counter("uniqueKeys");
    private final AtomicBoolean closed = new AtomicBoolean(false);
    
    public static void main(String[] args) throws IOException, InterruptedException {
        KafkaTester app = CommandLine.populateCommand(new KafkaTester(), args);
        CommandLine.run(app, args);
    }

    @Override
    public void run() {
        final ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(frequency, TimeUnit.SECONDS);

        Properties config = new Properties();
        config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        config.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        if (kafkaParameters != null) {
            kafkaParameters.forEach(option -> {
                String[] pair = option.split("=");
                config.setProperty(pair[0], pair[1]);
            });
        }
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(config);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            closed.set(true);
            LOG.info("Received shutdown request...");
        }));

        LOG.info("Connecting to topic {} on kafka server {}...", kafkaTopic, kafkaServers);
        consumer.subscribe(Arrays.asList(kafkaTopic));

        try {
            while (!closed.get()) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty()) {
                    messagesMeter.mark(records.count());
                    records.forEach(r -> uniqueKeys.add(r.key()));
                    uniqueKeyMeter.inc(uniqueKeys.size() - uniqueKeyMeter.getCount());
                }
            }
        } finally {
            consumer.close();
            reporter.close();
        }
    }

}
