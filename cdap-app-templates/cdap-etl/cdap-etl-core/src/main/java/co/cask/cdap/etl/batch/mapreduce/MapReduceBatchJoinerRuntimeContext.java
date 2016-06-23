package co.cask.cdap.etl.batch.mapreduce;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.batch.BatchJoinerRuntimeContext;

import java.util.Map;

/**
 * Created by vinishavyasa on 6/22/16.
 */
public class MapReduceBatchJoinerRuntimeContext extends MapReduceRuntimeContext implements BatchJoinerRuntimeContext {
  private final Map<String, Schema> inputSchemas;

  public MapReduceBatchJoinerRuntimeContext(MapReduceTaskContext context, Metrics metrics, LookupProvider lookup,
                                            String stageName, Map<String, String> runtimeArgs,
                                            Map<String, Schema> inputSchemas) {
    super(context, metrics, lookup, stageName, runtimeArgs);
    this.inputSchemas = inputSchemas;
  }

  @Override
  public Map<String, Schema> getInputSchemas() {
    return inputSchemas;
  }
}
