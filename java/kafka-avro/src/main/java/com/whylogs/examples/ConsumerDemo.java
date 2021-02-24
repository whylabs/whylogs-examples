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
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

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
        final Instant now = Instant.now();

        // map for storing the result
        final Map<Instant, DatasetProfile> result = new HashMap<>();

        try (final KafkaConsumer<String, LendingClubRow> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            while (true) {
                final ConsumerRecords<String, LendingClubRow> records = consumer.poll(Duration.ofMillis(1000));
                System.out.format("Read %d records\n", records.count());
                for (final ConsumerRecord<String, LendingClubRow> record : records) {
                    final String key = record.key();
                    final LendingClubRow value = record.value();
                    System.out.printf("key = %s, value = %s%n", key, value);
                }
                if (records.count() == 0)
                        break;
            }

        }

//        try (final InputStreamReader is = new InputStreamReader(ConsumerDemo.class.getResourceAsStream(INPUT_FILE_NAME))) {
//            final CSVParser parser = new CSVParser(is, CSV_FORMAT);
//
//            // iterate through records
//            for (final CSVRecord record : parser) {
//                // extract date time
//                final Instant dataTime = parseAndTruncateToYear(record.get(DATE_COLUMN));
//
//                // create new dataset profile
//                final DatasetProfile profile = result.computeIfAbsent(dataTime,
//                        t -> new DatasetProfile(sessionId, now, t, Collections.emptyMap(), Collections.emptyMap()));
//
//                // track multiple features
//                profile.track(record.toMap());
//            }
//        }

        System.out.println("Number of profiles: " + result.size());

        // write to a folder called "output"
        final Path output = Paths.get("output");
        Files.createDirectories(output);

        for (Map.Entry<Instant, DatasetProfile> entry : result.entrySet()) {
            final DatasetProfile profile = entry.getValue();
            // associate the year with filename
            final String fileName = String.format("profile_%s.bin", entry.getKey().atZone(ZoneOffset.UTC).getYear());

            // write out the output
            try (final OutputStream os =
                         Files.newOutputStream(output.resolve(fileName), StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
                profile.toProtobuf().build().writeDelimitedTo(os);
            }
        }
    }

    /**
     * Parse a text to an Instant object. This is used to extract data from the CSV and map
     * them into DatasetProfile's dataset_timestamp
     *
     * @param text input text
     * @return time in UTC as {@link Instant}
     */
    private static Instant parseAndTruncateToYear(String text) {
        return LocalDate.parse(text, DATE_TIME_FORMAT)
                .atStartOfDay()
                .withDayOfMonth(1)
                .withMonth(1)
                .atZone(ZoneOffset.UTC).toInstant();
    }
}
