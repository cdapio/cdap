package co.cask.cdap.etl.batch.mapreduce;

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;

/**
 * Needed because we have to do context.write(key, value) if there is only one output.
 *
 * See CDAP-3628 for more detail.
 *
 * @param <KEY_OUT> output key type
 * @param <VAL_OUT> output value type
 */
class SingleOutputWriter<KEY_OUT, VAL_OUT> extends SinkWriter<KEY_OUT, VAL_OUT> {

  protected SingleOutputWriter(MapReduceTaskContext<KEY_OUT, VAL_OUT> context) {
    super(context);
  }

  public void write(String sinkName, KeyValue<KEY_OUT, VAL_OUT> output) throws Exception {
    context.write(output.getKey(), output.getValue());
  }
}
