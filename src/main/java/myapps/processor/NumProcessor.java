package myapps.processor;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import static java.lang.System.currentTimeMillis;

public class NumProcessor implements Processor<String, Long, String, Long> {

    private ProcessorContext<String, Long> context;
    private KeyValueStore<String, Long> stateStore;
    private String stateStoreName;

    public NumProcessor(String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }
  
    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext<String, Long> context) {
        this.context = context;
        this.stateStore = context.getStateStore(this.stateStoreName);
    }
  
    public void process(final Record<String, Long> record) {
        Long num = record.value();
        Long currMax = stateStore.get("max");

        if (currMax == null || currMax < num) {
          System.out.println("currMax == null || currMax < num:" + currMax + " / " + num );
          currMax = num;
          System.out.println("currMax:" + currMax);
          stateStore.put("max", currMax);

          Record<String, Long> newMaxRecord = new Record<>(Long.toString(currMax), currMax, currentTimeMillis());

          context.forward(newMaxRecord);
        }
    }

    @Override
    public void close() {
        // close any resources managed by this processor
        // Note: Do not close any StateStores as these are managed by the library
    }
}