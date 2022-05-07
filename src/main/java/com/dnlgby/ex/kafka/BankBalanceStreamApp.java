package com.dnlgby.ex.kafka;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class BankBalanceStreamApp {

    private static final Gson gson = new Gson();

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank_balance_streams_app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        // Disable cache for demonstration purposes.
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();

        // Get the transactions stream
        KStream<String, String> transactionsStream = builder.stream("bank_transactions");

        // Perform parsing and grouping of the data
        KGroupedStream<String, Double> groupedStream = transactionsStream.map((key, value) -> new KeyValue<>(
                        gson.fromJson(value, BankTransaction.class).getName(),
                        gson.fromJson(value, BankTransaction.class).getAmount())).
                peek((name, value) -> System.out.println("name: " + name + " value: " + value)).
                groupByKey(Grouped.with(Serdes.String(), Serdes.Double()));

        // Aggregate people balances
        KTable<String, Double> balancesTable = groupedStream.aggregate(() -> 0.0,
                (key, value, aggregate) -> aggregate + value,
                Materialized.with(Serdes.String(), Serdes.Double()));

        // Send current balances to the 'bank_balances' topic
        balancesTable.toStream().to("bank_balances");


        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        // Demonstration purposes
        streams.cleanUp();

        streams.start();

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

}
