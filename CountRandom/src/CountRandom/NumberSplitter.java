package CountRandom;

import com.continuuity.api.flow.flowlet.*;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import com.continuuity.api.flow.flowlet.builders.TupleSchemaBuilder;

public class NumberSplitter extends AbstractComputeFlowlet {
  @Override
  public void process(Tuple tuple, TupleContext tupleContext, OutputCollector outputCollector) {
    Integer i = tuple.get("number");
    outputCollector.emit(new TupleBuilder().set("number", new Integer(i % 10000)).create());
    outputCollector.emit(new TupleBuilder().set("number", new Integer(i % 1000)).create());
    outputCollector.emit(new TupleBuilder().set("number", new Integer(i % 100)).create());
    outputCollector.emit(new TupleBuilder().set("number", new Integer(i % 10)).create());
  }

  @Override
  public void configure(StreamsConfigurator streamsConfigurator) {
    TupleSchema inout = new TupleSchemaBuilder().
        add("number", Integer.class).
        create();
    streamsConfigurator.getDefaultTupleInputStream().setSchema(inout);
    streamsConfigurator.getDefaultTupleOutputStream().setSchema(inout);
  }
}
