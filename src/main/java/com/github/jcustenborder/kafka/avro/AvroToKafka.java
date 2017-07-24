/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.avro;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class AvroToKafka implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(AvroToKafka.class);

  final Producer producer;

  public AvroToKafka(Properties properties) {
    this.producer = new KafkaProducer(properties);
  }

  Schema readSchema(File file) throws IOException {
    log.info("readSchema() - reading schema from '{}'", file);

    try (DataFileReader<Void> reader = new DataFileReader<>(file, new GenericDatumReader<Void>())) {
      return reader.getSchema();
    }
  }

  public void execute(File avroFile) throws IOException {
    Schema schema = readSchema(avroFile);
    Schema.Field keyField = schema.getField("key");
    Schema.Field valueField = schema.getField("value");
    Schema.Field timestampField = schema.getField("timestamp");
    Schema.Field topicField = schema.getField("topic");


    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
    GenericRecord genericRecord = null;
    try (DataFileReader<GenericRecord> reader = new DataFileReader<GenericRecord>(avroFile, datumReader)) {
      while (null != (genericRecord = reader.next(genericRecord))) {
        final Object key = genericRecord.get(keyField.name());
        final Object value = genericRecord.get(valueField.name());
        final long timestamp = (long) genericRecord.get(timestampField.name());
        final String topic = (String) genericRecord.get(topicField.name());


        ProducerRecord producerRecord = new ProducerRecord(
            topic,
            null,
            timestamp,
            key,
            value
        );

        this.producer.send(producerRecord);
      }
    }
  }


  @Override
  public void close() throws Exception {
    this.producer.close();
  }

  public static void main(String... args) throws Exception {
    if (args.length < 2) {
      System.out.println("Arguments:");
      System.out.println("producer.properties data1.avro <data2.avro> <data3.avro>");
      System.exit(1);
      return;
    }

    File configFile = new File(args[0]);
    List<File> dataFiles = new ArrayList<>();
    for (int i = 1; i < args.length; i++) {
      String f = args[i];
      dataFiles.add(new File(f));
    }

    Properties properties = new Properties();
    try (InputStream inputStream = new FileInputStream(configFile)) {
      properties.load(inputStream);
    }
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

    try (AvroToKafka avroToKafka = new AvroToKafka(properties)) {
      for (File datafile : dataFiles) {
        avroToKafka.execute(datafile);
      }
    }
  }
}
