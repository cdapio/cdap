package CountRandom;

import com.continuuity.api.data.Increment;
import com.continuuity.api.flow.flowlet.*;
import com.continuuity.api.flow.flowlet.builders.TupleSchemaBuilder;

public class NumberCounter extends ComputeFlowlet {

  @Override
  public void process(Tuple tuple, TupleContext tupleContext, OutputCollector outputCollector) {
    Integer i = tuple.get("number");
    getFlowletContext().getLogger().info("Processing integer " + i.intValue());
    outputCollector.add(new Increment(i.toString().getBytes(), 1L));
  }

  @Override
  public void configure(StreamsConfigurator streamsConfigurator) {
    TupleSchema in = new TupleSchemaBuilder().
        add("number", Integer.class).
        create();
    streamsConfigurator.getDefaultTupleInputStream().setSchema(in);
  }

}
