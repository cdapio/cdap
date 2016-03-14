package co.cask.cdap.etl.batch.mapreduce;

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;

/**
 * Wrapper around sinks to help writing sink output to the correct named output.
 * Would not be needed if you could always do context.write(name, key, value),
 * but if there is just one output, you must do context.write(key, value) instead of context.write(name, key, value).
 *
 * See CDAP-3628 for more detail.
 *
 * @param <KEY_OUT> output key type
 * @param <VAL_OUT> output value type
 */
abstract class SinkWriter<KEY_OUT, VAL_OUT> {
  protected final MapReduceTaskContext<KEY_OUT, VAL_OUT> context;

  public SinkWriter(MapReduceTaskContext<KEY_OUT, VAL_OUT> context) {
    this.context = context;
  }

  protected abstract void write(String sinkName, KeyValue<KEY_OUT, VAL_OUT> output) throws Exception;
}
