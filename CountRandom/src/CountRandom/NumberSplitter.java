package CountRandom;

import com.continuuity.api.flow.flowlet.*;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import com.continuuity.api.flow.flowlet.builders.TupleSchemaBuilder;

public class NumberSplitter extends ComputeFlowlet {
  @Override
  public void process(Tuple tuple, TupleContext tupleContext, OutputCollector outputCollector) {
    Integer i = tuple.get("number");
    outputCollector.add(new TupleBuilder().
        set("number", new Integer(i % 10000)).
        create());
    outputCollector.add(new TupleBuilder().
        set("number", new Integer(i % 1000)).
        create());
    outputCollector.add(new TupleBuilder().
        set("number", new Integer(i % 100)).
        create());
    outputCollector.add(new TupleBuilder().
        set("number", new Integer(i % 10)).
        create());
  }

  @Override
  public void configure(FlowletSpecifier specifier) {
    TupleSchema inout = new TupleSchemaBuilder().
        add("number", Integer.class).
        create();
    specifier.getDefaultFlowletInput().setSchema(inout);
    specifier.getDefaultFlowletOutput().setSchema(inout);
  }
}
