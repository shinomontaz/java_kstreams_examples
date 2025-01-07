package myapps;

import myapps.processor.NumProcessor;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.kstream.Reducer;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class MaxNums {

    public static void main(String[] args) throws Exception {

        final Serde<Long> longSerde = Serdes.Long();
        final Serde<String> stringSerde = Serdes.String();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-maxnums3");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9921,localhost:9922,localhost:9923");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, longSerde.getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "D:/vbshared/streams-quickstart/store-maxnums");

        final StreamsBuilder builder = new StreamsBuilder();

        final String stateStoreOdds = "maxnum-odds-store";
        StoreBuilder<KeyValueStore<String, Long>> maxOddsStore = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(stateStoreOdds),
            stringSerde,
            longSerde);
        final String stateStoreEvens = "maxnum-evens-store";
            StoreBuilder<KeyValueStore<String, Long>> maxEvensStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(stateStoreEvens),
                stringSerde,
                longSerde);
    
        builder.addStateStore(maxOddsStore);
        builder.addStateStore(maxEvensStore);

        KStream<String, Long> source = builder.stream("example-nums-int", Consumed.with(stringSerde, longSerde));

        source.filter((key, value) -> {
            return value%2 == 0;
        }).selectKey((k,v) -> "odd")
        .groupByKey()
        .reduce(new Reducer<Long>() {
                    @Override
                    public Long apply(Long currentMax, Long v) {
                        Long max = (currentMax > v) ? currentMax : v;
                        return max;
                    }
        }).toStream().to("max-odd-output");

        source.filter((key, value) -> {
            return value%2 != 0;
        }).selectKey((k,v) -> "even")
        .groupByKey()
        .reduce(new Reducer<Long>() {
            @Override
            public Long apply(Long currentMax, Long v) {
                Long max = (currentMax > v) ? currentMax : v;
                return max;
            }
        }).toStream().to("max-even-output");
        
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