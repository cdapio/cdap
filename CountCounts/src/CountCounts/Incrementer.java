package CountCounts;

import com.continuuity.api.data.Increment;
import com.continuuity.api.flow.flowlet.*;
import com.continuuity.api.flow.flowlet.builders.*;

public class Incrementer extends AbstractComputeFlowlet
{
  @Override
  public void configure(StreamsConfigurator configurator) {
    TupleSchema in = new TupleSchemaBuilder().
        add("count", Integer.class).
        create();
    configurator.getDefaultTupleInputStream().setSchema(in);
  }

  @Override
  public void process(Tuple tuple, TupleContext tupleContext, OutputCollector outputCollector) {
    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() + ": Received tuple " + tuple);

    Integer count = tuple.get("count");
    if (count == null) return;
    String key = Integer.toString(count);

    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() + ": Emitting " +
          "Increment for " + key);

    Increment increment = new Increment(key.getBytes(), 1);
    outputCollector.emit(increment);
  }
}
