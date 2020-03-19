package com.sample;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleStreams {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple-streams");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "2");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.DEBUG.toString());

        // After 2.4, you can configure metrics to either be in compatibility mode or show the new metrics
        // Do not use these configuration options until using a version > 2.4
        // See https://github.com/apache/kafka/blob/trunk/streams/src/main/java/org/apache/kafka/streams/StreamsConfig.java#L320
        // props.put(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, StreamsConfig.METRICS_0100_TO_24);
        // props.put(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, StreamsConfig.METRICS_LATEST);


        final StreamsBuilder sb = new StreamsBuilder();

        final KStream<String, String> source = sb.stream("sample", Consumed.with(
                Serdes.String(),
                Serdes.String()
        ));

        final KStream<String, Long> numbers = source.mapValues((readOnlyKey, value) -> Long.valueOf(value.substring(6)));

        numbers.to("numbers", Produced.with(Serdes.String(), Serdes.Long()));

        final KStream<Long, String> evenodd = numbers.map((key, value) -> new KeyValue<>(value, value % 2 == 0 ? "even" : "odd"));

        evenodd.to("evenodd", Produced.with(Serdes.Long(), Serdes.String()));

        final Topology top = sb.build();

        System.out.println(top.describe().toString());

        final KafkaStreams streams = new KafkaStreams(top, props);

        final AtomicBoolean shouldStop = new AtomicBoolean(false);

        streams.setUncaughtExceptionHandler((t, e) -> {
            System.out.println("Uncaught exception encountered");
            e.printStackTrace();
            shouldStop.set(true);
        });

        streams.start();

        while (!shouldStop.get()) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        streams.close();
    }
}
