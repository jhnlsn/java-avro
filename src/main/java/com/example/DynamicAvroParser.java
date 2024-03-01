package com.example;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;

public class DynamicAvroParser {
        public static void main(String[] args) throws IOException {

        Schema schemaAccount = new Schema.Parser().parse(new File("src/main/resources/avro/person.avsc"));

        // Path to the Avro file
        String pathAccount = "dynamic_persons.avro";

        // Write data dynamically to an Avro file
        // Uncomment this out to write data to the Avro file
        writeGenericRecords(schemaAccount, pathAccount);

        // // Read data dynamically from an Avro file
        // readGenericRecords(schema, path);

        // readSpecificRecords(schemaAccount, pathAccount);
    }

    public static void writeGenericRecords(Schema schema, String path) throws IOException {
        GenericRecord account = new GenericData.Record(schema);
        account.put("firstName", "John");

        File file = new File(path);
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, file);
            dataFileWriter.append(account);
        }
    }

    public static void readGenericRecords(Schema schema, String path) throws IOException {
        
        File file = new File(path);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader)) {
            GenericRecord user = null;
            while (dataFileReader.hasNext()) {
                user = dataFileReader.next(user);
                System.out.println(user);
            }
        }
    }

    public static void readSpecificRecords(Schema schema, String path) throws IOException {
        try {
            // Specify the path to the Avro file
            File file = new File(path);

            // DatumReader is used to de-serialize Avro data from disk
            SpecificDatumReader<PersonCreated> datumReader = new SpecificDatumReader<>(PersonCreated.class);

            // Construct a DataFileReader instance to read the data
            try (DataFileReader<PersonCreated> dataFileReader = new DataFileReader<>(file, datumReader)) {
                PersonCreated account = null; // Reusable Account object

                // Iterate over the file and print each Account object
                while (dataFileReader.hasNext()) {
                    // Reuse user object by passing it to next(). This saves us from
                    // allocating and garbage collecting many objects for files with many items
                    account = dataFileReader.next(account);
                    System.out.println(account);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
