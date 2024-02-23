package com.whylogs.examples;

import com.whylogs.LendingClubRow;
import com.whylogs.core.DatasetProfile;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static java.time.Instant.now;

/**
 * An example of processing a CSV dataset.
 *
 * Here we demonstrate how you can extract data from a CSV file and track it with WhyLogs. We group
 * the data by year here and run profiling for each year.
 *
 * In practice, if the data is sorted by date, you can write the data to disk as soon as you see the timestamp
 * increase (in this case, you see the value of the following year in the dataset). In that way you can
 * guarantee constant memory usage.
 */
public class ConsumerDemo {
    public static final String DATE_COLUMN = "Call Date";
    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT
            .withFirstRecordAsHeader()
            .withNullString("")
            .withDelimiter(',');
    public static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("MM/dd/yyyy");

    private static final String TOPIC = "whylogs-events";
    //private static final String TOPIC = "transactions";

    private static final Properties props = new Properties();
    private static String configFile;

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            // Backwards compatibility, assume localhost
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        } else {
            // Load properties from a local configuration file
            // Create the configuration file (e.g. at '$HOME/.confluent/java.config') with configuration parameters
            // to connect to your Kafka cluster, which can be on your local host, Confluent Cloud, or any other cluster.
            // Documentation at https://docs.confluent.io/platform/current/tutorials/examples/clients/docs/java.html
            configFile = args[0];
            if (!Files.exists(Paths.get(configFile))) {
                throw new IOException(configFile + " not found.");
            } else {
                try (InputStream inputStream = new FileInputStream(configFile)) {
                    props.load(inputStream);
                }
            }
        }

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ctest-payments");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        final String sessionId = UUID.randomUUID().toString();
        final Instant now = now();
        final DatasetProfile profile = new DatasetProfile(sessionId, now, null, Collections.emptyMap(), Collections.emptyMap());

        try (final KafkaConsumer<String, LendingClubRow> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));
            int count = 0;
            while (true) {
                final ConsumerRecords<String, LendingClubRow> records = consumer.poll(Duration.ofMillis(1000));
                System.out.format("Read %d records\n", records.count());
                count += records.count();
                for (final ConsumerRecord<String, LendingClubRow> record : records) {
                    final String key = record.key();
                    final LendingClubRow value = record.value();
                    Map<String, Object> map = new HashMap<>();
                    value.getClassSchema().getFields().forEach(field ->
                            map.put(field.name(), value.get(field.name())));

                    // track multiple features
                    profile.track(map);
                }
                if (records.count() == 0)
                        break;
            }
            System.out.format("Received %d events\n", count);
        }

        // write to a folder called "output"
        final Path output = Paths.get("output");
        Files.createDirectories(output);

        // associate the year with filename
        final String fileName = String.format("profile_%s.bin", Instant.now().atZone(ZoneOffset.UTC).getYear());

        // write out the output
        System.out.format("Writing profile to %s\n", fileName);
        try (final OutputStream os =
                     Files.newOutputStream(output.resolve(fileName), StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            profile.toProtobuf().build().writeDelimitedTo(os);
        }

    }
}
