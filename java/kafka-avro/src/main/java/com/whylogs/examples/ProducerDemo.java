package com.whylogs.examples;

import com.datamelt.csv.avro.CsvToAvroGenericWriter;
import com.whylogs.LendingClubRow;
import com.whylogs.core.DatasetProfile;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;

import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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
public class ProducerDemo {

    public static final String DATE_COLUMN = "Call Date";
    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT
            .withFirstRecordAsHeader()
            .withNullString("")
            .withDelimiter(',');
    public static final String INPUT_FILE_NAME = "lending_club_1000.csv";
    public static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("MM/dd/yyyy");

    private static final String TOPIC = "whylogs-events";
    private static final Properties props = new Properties();
    private static String configFile;


    public static void main(String[] args) throws Exception {
        final String sessionId = UUID.randomUUID().toString();
        final Instant now = Instant.now();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");

        // map for storing the result
        final Map<Instant, DatasetProfile> result = new HashMap<>();
        System.out.println("opening " + INPUT_FILE_NAME);


        try (KafkaProducer producer = new KafkaProducer(props)) {

            try (final InputStreamReader is = new InputStreamReader(ProducerDemo.class.getResourceAsStream(INPUT_FILE_NAME))) {
                final CSVParser parser = new CSVParser(is, CSV_FORMAT);
                final LendingClubRow value = new LendingClubRow();
                final Schema schema = value.getSchema();
                final CsvToAvroGenericWriter csv2avro = new CsvToAvroGenericWriter(schema);

                // iterate through records
                for (final CSVRecord record : parser) {
                    final String orderId = "id" + Long.toString(1);

                    GenericRecord avroRecord = csv2avro.populate(record.iterator());

                    final ProducerRecord<Object, Object> precord = new ProducerRecord<>(TOPIC, "", avroRecord);

                    producer.send(precord);
                    System.out.println("Sent event...");

                }
            }
        } catch (final SerializationException e) {
            e.printStackTrace();
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
