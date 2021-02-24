package com.datamelt.csv.avro;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;

import org.joda.time.DateTime;

public class CsvToAvroGenericWriter {
    // constants which define the type of separator used in the csv file
    // to separate the individual columns
    public static final String SEPARATOR_TAB = "\t";
    public static final String SEPARATOR_SEMICOLON = ";";
    public static final String SEPARATOR_COMMA = ",";
    public static final String SEPARATOR_AT = "@";
    public static final String SEPARATOR_AT_AT = "@@";
    public static final String SEPARATOR_PIPE = "|";

    // we can write to a new file respectively overwrite an existing one
    // or append to an existing one
    public static final int MODE_WRITE = 0;
    public static final int MODE_APPEND = 1;

    // the default mode is write
    private static int mode = MODE_WRITE;

    // default compression factor
    public static final int DEFAULT_COMPRESSION = 6;
    private static Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);
    private Schema avroSchema = null;
    private String[] csvHeaderFields = null;
    private Map<String, Integer> fieldMap = null;
    private String separator = SEPARATOR_SEMICOLON;

    private String csvDateTimeFormat;
    private String csvDateFormat;

    /**
     * Constructor to accept the Avro schema file and the output file name
     * to write the avro data to. Default compression of 6 will be used.
     * <p>
     * Pass the Avro schema and the path and name of the output file and the
     * mode (write or append).
     * <p>
     * An Avro DataFileWriter object will be created for the given output file,
     * using the default compression.
     *
     * @param schema     avro schema to use
     * @throws Exception
     */
    public CsvToAvroGenericWriter(Schema schema) throws Exception {
        this.avroSchema = schema;
        this.populateFieldMap();
    }


    /**
     * add all fields and their positions from the Avro schema to a map
     * for easy retrieval
     */
    private void populateFieldMap() {
        // populate the map of avro field names and positions
        fieldMap = new HashMap<String, Integer>();
        List<Schema.Field> avroFields = avroSchema.getFields();
        for (int i = 0; i < avroFields.size(); i++) {
            Schema.Field avroField = avroFields.get(i);
            fieldMap.put(avroField.name(), avroField.pos());
        }
    }


    /**
     * sets the format of datetime fields as it is used in the CSV file.
     * <p>
     * if the relevant field is defined as "long" with a logical type "timestamp-millis" this
     * format will be used to parse the csv value and convert it to milliseconds from the unix epoch
     *
     * @param format
     */
    public void setCsvDateTimeFormat(String format) {
        this.csvDateTimeFormat = format;
    }


    public String getCsvDateTimeFormat() {
        return csvDateTimeFormat;
    }

    /**
     * sets the format of date fields as it is used in the CSV file.
     * <p>
     * if the relevant field is defined as "int" with a logical type "date" this
     * format will be used to parse the csv value and convert it to days from the unix epoch
     *
     * @param format
     */
    public void setCsvDateFormat(String format) {
        this.csvDateFormat = format;
    }


    public String getCsvDateFormat() {
        return csvDateFormat;
    }

    /**
     * sets the header row taken from the csv file.The given
     * separatorCharacter is used to divide the header into individual fields.
     * <p>
     * if the header row is defined, the append() method will
     * correctly add the correct values from the CSV file to the
     * Avro record, even if the CSV files has a different sequence
     * of fields.
     *
     * @param header
     * @param separatorCharacter
     */
    public void setCsvHeader(String header, String separatorCharacter) {
        this.csvHeaderFields = header.split(separatorCharacter);
    }

    public void setCsvHeader(String[] fields) {
        this.csvHeaderFields = fields;
    }

    /**
     * sets the header row taken from the csv file. The default
     * separator is used to divide the header into individual fields.
     * <p>
     * if the header row is defined, the append() method will
     * correctly add the correct values from the CSV file to the
     * Avro record, even if the CSV files has a different sequence
     * of fields.
     *
     * @param header
     */
    public void setCsvHeader(String header) {
        setCsvHeader(header, separator);
    }


    /**
     * returns a list of fields that can be null based on
     * the Avro schema.
     *
     * @param record
     * @return
     */
    private ArrayList<Schema.Field> getInvalidNullFields(GenericRecord record) {
        List<Schema.Field> avroFields = avroSchema.getFields();
        ArrayList<Schema.Field> nullFields = new ArrayList<Schema.Field>();
        for (int i = 0; i < avroFields.size(); i++) {
            Schema.Field field = avroFields.get(i);
            Object value = record.get(field.pos());

            if (value == null && !getFieldAllowsNull(field)) {
                nullFields.add(field);
            }
        }
        return nullFields;
    }

    private String[] getSplitLine(String line, String separatorCharacter) {
        return line.split(separatorCharacter);
    }

    /**
     * populates an Avro Generic record with the values from a row of CSV data.
     * <p>
     * If the header row of the CSV is undefined, then it is assumed that the fields
     * in the CSV file are present in the same sequence as they are defined in the
     * Avro schema,
     * <p>
     * If the header row is defined, then this method will locate the corresponding Avro
     * field with the same name in the schema. In this case the sequence of fields in the
     * CSV file is not relevant - the method will update the correct Avro field.
     *
     * @param fields a line/row of data from a CSV file as an array of Strings
     * @return a record with the data of a line from the CSV file
     */
    public GenericRecord populate(String[] fields) throws Exception {
        return this.populate(Arrays.asList(fields).iterator());
    }

    public GenericRecord populate(Iterator<String> fields) throws Exception {

            GenericRecord record = new GenericData.Record(avroSchema);
        // if the names of the fields are defined (header row was specified)
        if (csvHeaderFields != null) {
            int i = 0;
            // loop of the header fields
            while (fields.hasNext()) {
                // name of the csv field as defined in the header
                String csvFieldName = csvHeaderFields[i];

                // if the field name from the CSV file is present in the Avro schema.
                // if the equivalent field is not found in the avro schema, then we
                // ignore it
                if (fieldMap.containsKey(csvFieldName)) {
                    // get the position of the field with the same name
                    // in the avro schema
                    int avroPosition = fieldMap.get(csvFieldName);

                    // retrieve the field
                    Schema.Field field = avroSchema.getField(csvFieldName);

                    // retrieve a field from the Avro SpecificRecord
                    Object object = getObject(field, fields.next());
                    // add the object to the corresponding field
                    record.put(avroPosition, object);
                }
                i++;
            }
        } else {
            List<Schema.Field> avroFields = avroSchema.getFields();
            int i = 0;
            while (fields.hasNext()) {
                Schema.Field field = avroFields.get(i);
                // retrieve a field from the Avro SpecificRecord
                String csf = fields.next();
                Object object = getObject(field, csf);
                // add the object to the corresponding field
                record.put(i, object);
                i++;
            }
        }
        return record;
    }

    /**
     * converts the String value from the CSV file into the correct
     * object (type) according to the Avro schema definition.
     * <p>
     * if the value is null and null is allowed per schema definition then
     * null is returned
     * <p>
     * if the string value can not be converted to the appropriate type in
     * the schema, an exception is thrown.
     * <p>
     * if the value is an empty string, 0 respectively 0.0 is returned for fields
     * of type Integer, Long, Float or Double.
     *
     * @param field a field from the Avro record
     * @param value the value from a CSV
     * @return the correct object according to the schema
     */
    private Object getObject(Schema.Field field, String value) throws Exception {
        // retrieve the field type
        Schema.Type fieldType = getFieldType(field);

        // retrieve the logical type of the field. relevant for some date and time types
        // read the documentation at: avro.apache.org
        LogicalType logicalFieldType = getFieldLogicalType(field);

        boolean nullAllowed = getFieldAllowsNull(field);

        if (value != null && !value.equals("")) {
            if (fieldType == Schema.Type.INT) {
                try {
                    if (logicalFieldType != null && logicalFieldType.getClass() == LogicalTypes.Date.class) {
                        return convertToDays(value);
                    } else {
                        return Integer.parseInt(value);
                    }
                } catch (Exception ex) {
                    throw new Exception("value [" + value + "] of field [" + field.name() + "] could not be converted to an integer");
                }
            } else if (fieldType == Schema.Type.LONG) {
                try {
                    if (logicalFieldType != null && logicalFieldType.getClass() == LogicalTypes.TimestampMillis.class) {
                        return convertToTimestampMilliseconds(value);
                    } else {
                        return Long.parseLong(value);
                    }
                } catch (Exception ex) {
                    throw new Exception("value [" + value + "] of field [" + field.name() + "] could not be converted to a long");
                }
            } else if (fieldType == Schema.Type.FLOAT) {
                try {
                    return Float.parseFloat(value);
                } catch (Exception ex) {
                    throw new Exception("value [" + value + "] of field [" + field.name() + "] could not be converted to a float");
                }
            } else if (fieldType == Schema.Type.DOUBLE) {
                try {
                    return Double.parseDouble(value);
                } catch (Exception ex) {
                    throw new Exception("value [" + value + "] of field [" + field.name() + "] can not be converted to a double");
                }
            } else if (fieldType == Schema.Type.BOOLEAN) {
                try {
                    return Boolean.parseBoolean(value);
                } catch (Exception ex) {
                    throw new Exception("value [" + value + "] of field [" + field.name() + "] can not be converted to a boolean");
                }
            } else if (fieldType == Schema.Type.STRING) {
                return value;
            } else {
                throw new Exception("type [" + fieldType + "] not supported. field: [" + field.name() + "]");
            }
        }
        // the value is either null or an empty string
        else {
            if (value != null && value.equals("")) {
                if (fieldType == Schema.Type.STRING) {
                    return value;
                } else if (fieldType == Schema.Type.INT) {
                    return 0;
                } else if (fieldType == Schema.Type.LONG) {
                    return (long) 0;
                } else if (fieldType == Schema.Type.DOUBLE) {
                    return (double) 0;
                } else if (fieldType == Schema.Type.FLOAT) {
                    return (float) 0;
                } else if (fieldType == Schema.Type.BOOLEAN) {
                    return Boolean.FALSE;
                } else {
                    throw new Exception("empty field value is not defined in the schema for field: [" + field.name() + "]");
                }

            }
            // must be null if we arrive here
            else {
                if (nullAllowed) {
                    return null;
                } else {
                    throw new Exception("field value of [null] is not defined in the schema for field: [" + field.name() + "]");
                }
            }
        }
    }

    /**
     * converts the given value to milliseconds from the unix epoch using the
     * defined format.
     * <p>
     * Note: datetime in an avro schema is defined in number of days
     * from the unix epoche
     *
     * @param value the value to convert
     * @throws Exception when the value can not be parsed to a long
     * @return milliseconds from the unix epoch as a long value
     */
    private long convertToTimestampMilliseconds(String value) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat(csvDateTimeFormat);
        try {
            Date date = sdf.parse(value);
            return new DateTime(date.getTime()).getMillis();
        } catch (Exception ex) {
            throw new Exception("the value [" + value + "] cannot be converted to milliseconds from the unix epoch using the specified format [" + csvDateTimeFormat + "]");
        }
    }

    /**
     * converts the given value to days from the unix epoch using the
     * defined format.
     * <p>
     * Note: dates in an avro schema are defined in number of days
     * from the unix epoche
     *
     * @param value the value to convert
     * @throws Exception when the value can not be parsed to a long
     * @return milliseconds from the unix epoch as a long value
     */
    private int convertToDays(String value) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat(csvDateFormat);
        try {
            Date date = sdf.parse(value);

            long currentMilli = date.getTime();
            long seconds = currentMilli / 1000;
            long minutes = seconds / 60;
            long hours = minutes / 60;
            long days = hours / 24;
            return (int) days;
        } catch (Exception ex) {
            throw new Exception("the value [" + value + "] cannot be converted to days from the unix epoch using the specified format [" + csvDateFormat + "]");
        }
    }


    /**
     * returns the type of the avro schema field
     *
     * @param field the avro schema field
     * @return the type that is defined for the field
     */
    private Schema.Type getFieldType(Schema.Field field) {
        Schema.Type fieldType = field.schema().getType();

        Schema.Type type = null;

        // if the field is of type union, we must loop to get the correct type
        // if not then there is only one definition
        if (fieldType == Schema.Type.UNION) {
            List<Schema> types = field.schema().getTypes();

            for (int i = 0; i < types.size(); i++) {
                // get the type that does NOT define null as the
                // possible type
                if (!types.get(i).equals(NULL_SCHEMA)) {
                    type = types.get(i).getType();
                    break;
                }
            }
            return type;
        } else {
            return fieldType;
        }
    }

    /**
     * returns the logical field type of an avro schema field
     * <p>
     * logical type definitions can help to further define which type
     * of field is used in the schema. e.g. a date or time field.
     * <p>
     * Note: there is a bug in avro 1.8.2. and datetime fields.
     * it is not clear when that will be fixed.
     *
     * @param field the avro schema field
     * @return a logical type definition if one was specified in the avro schema
     */
    private LogicalType getFieldLogicalType(Schema.Field field) {
        Schema.Type type = field.schema().getType();

        LogicalType logicalFieldType = null;

        if (type == Schema.Type.UNION) {
            List<Schema> types = field.schema().getTypes();

            if (types.get(0).equals(NULL_SCHEMA)) {
                logicalFieldType = types.get(1).getLogicalType();
            } else {
                logicalFieldType = types.get(0).getLogicalType();
            }
            return logicalFieldType;
        } else {
            return logicalFieldType;
        }
    }

    /**
     * determines if the definition of the field allows null as value
     *
     * @param field and avro schema field
     * @return if the specified field allows null
     */
    private boolean getFieldAllowsNull(Schema.Field field) {
        Schema.Type type = field.schema().getType();

        boolean nullAllowed = false;

        // the null is allowed we have two fields (maybe more): one for
        // the field type and one defining null
        if (type == Schema.Type.UNION) {
            List<Schema> types = field.schema().getTypes();

            for (int i = 0; i < types.size(); i++) {
                if (types.get(i).equals(NULL_SCHEMA)) {
                    nullAllowed = true;
                    break;
                }
            }
            return nullAllowed;
        } else {
            return nullAllowed;
        }
    }
}
