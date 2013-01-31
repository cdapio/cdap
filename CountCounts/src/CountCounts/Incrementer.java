package CountCounts;

import com.continuuity.api.data.Increment;
import com.continuuity.api.flow.flowlet.*;
import com.continuuity.api.flow.flowlet.builders.*;

public class Incrementer extends ComputeFlowlet
{
  static String keyTotal = ":sinkTotal:";

  @Override
  public void configure(FlowletSpecifier configurator) {
    TupleSchema in = new TupleSchemaBuilder().
        add("count", Integer.class).
        create();
    configurator.getDefaultFlowletInput().setSchema(in);
  }

  CounterTable counters;

  @Override
  public void initialize() {
    super.initialize();
    this.counters = getFlowletContext().getDataSet(Common.tableName);
  }

  @Override
  public void process(Tuple tuple, TupleContext tupleContext, OutputCollector outputCollector) {
    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Received tuple " + tuple);
    }
    Integer count = tuple.get("count");
    if (count == null) {
      return;
    }
    String key = Integer.toString(count);

    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Emitting " +
          "Increment for " + key);
    }
    // emit an increment for the number of words in this document
    this.counters.increment(key);

    if (Common.count) {
      // emit an increment for the total number of documents counted
      this.counters.increment(keyTotal);
    }
  }
}
