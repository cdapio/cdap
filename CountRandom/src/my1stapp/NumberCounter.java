package my1stapp;

import com.continuuity.api.data.Increment;
import com.continuuity.api.flow.flowlet.*;
import com.continuuity.flow.flowlet.internal.TupleSchemaBuilderImpl;

public class NumberCounter extends AbstractComputeFlowlet {

  @Override
  public void process(Tuple tuple, TupleContext tupleContext, OutputCollector outputCollector) {
    Integer i = tuple.get("number");
    outputCollector.emit(new Increment(i.toString().getBytes(), 1L));
  }

  @Override
  public void configure(StreamsConfigurator streamsConfigurator) {
    TupleSchema in = new TupleSchemaBuilderImpl().
        add("number", Integer.class).
        create();
    streamsConfigurator.getDefaultTupleInputStream().setSchema(in);
  }

}
