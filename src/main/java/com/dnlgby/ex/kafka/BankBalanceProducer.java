package com.dnlgby.ex.kafka;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class BankBalanceProducer {

    private static final Logger log = LoggerFactory.getLogger(BankBalanceProducer.class.getSimpleName());
    private static final Gson gson = new Gson();
    private static final int sleepTime = 1000;

    public static void main(String[] args) {

        log.info("Bank balance producer start");

        final String bankBalanceTopicName = "bank_transactions";
        final int numberOfMessages = 100;

        // create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Add shutdown hook to flush and close producer
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));

        while (true) {
            for (int i = 0; i < numberOfMessages; i++) {

                // create a producer record
                BankTransaction curTransaction = BankTransaction.createRandom();
                String curTransactionStr = gson.toJson(curTransaction);

                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(bankBalanceTopicName, curTransactionStr);

                // send the data - asynchronous
                producer.send(producerRecord, (metadata, e) -> {
                    // executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        // the record was successfully sent
                        log.info("Received new metadata. \n" +
                                 "Topic: " + metadata.topic() + "\n" +
                                 "Partition: " + metadata.partition() + "\n" +
                                 "Offset: " + metadata.offset() + "\n" +
                                 "Timestamp: " + metadata.timestamp());
                    } else {
                        log.error("Error while producing", e);
                    }
                });

            }

            // Sleep for 1 second.
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }


    }
}
