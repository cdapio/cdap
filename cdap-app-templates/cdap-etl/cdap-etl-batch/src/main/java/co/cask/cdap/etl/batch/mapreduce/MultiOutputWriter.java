package co.cask.cdap.etl.batch.mapreduce;

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;

import java.util.Map;

/**
 * Writes sink output to the correct named output.
 *
 * @param <KEY_OUT> output key type
 * @param <VAL_OUT> output value type
 */
class MultiOutputWriter<KEY_OUT, VAL_OUT> extends SinkWriter<KEY_OUT, VAL_OUT> {
  // sink name -> outputs for that sink
  private final Map<String, SinkOutput> sinkOutputs;

  public MultiOutputWriter(MapReduceTaskContext<KEY_OUT, VAL_OUT> context, Map<String, SinkOutput> sinkOutputs) {
    super(context);
    this.sinkOutputs = sinkOutputs;
  }

  public void write(String sinkName, KeyValue<KEY_OUT, VAL_OUT> output) throws Exception {
    for (String outputName : sinkOutputs.get(sinkName).getSinkOutputs()) {
      context.write(outputName, output.getKey(), output.getValue());
    }
  }
}
