package myapps;

import myapps.processor.NumProcessor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class MaxInt {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-maxint4");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9921,localhost:9922,localhost:9923");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "D:/vbshared/streams-quickstart/store3");

        final StreamsBuilder builder = new StreamsBuilder();

        final String stateStoreName = "maxnum-store";
        StoreBuilder<KeyValueStore<String, Long>> maxNumStore = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(stateStoreName),
            Serdes.String(),
            Serdes.Long());

        builder.addStateStore(maxNumStore);

        KStream<String, Long> source = builder.stream("streams-long-nums");
        
        source.process(NumProcessor::new, stateStoreName).to("example-nums-output");

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}