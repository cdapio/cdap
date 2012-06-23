package my1stapp;

import com.continuuity.api.flow.flowlet.*;
import com.continuuity.flow.flowlet.internal.TupleBuilderImpl;
import com.continuuity.flow.flowlet.internal.TupleSchemaBuilderImpl;

public class NumberSplitter extends AbstractComputeFlowlet {
  @Override
  public void process(Tuple tuple, TupleContext tupleContext, OutputCollector outputCollector) {
    Integer i = tuple.get("number");
    outputCollector.emit(new TupleBuilderImpl().set("number", new Integer(i % 10000)).create());
    outputCollector.emit(new TupleBuilderImpl().set("number", new Integer(i % 1000)).create());
    outputCollector.emit(new TupleBuilderImpl().set("number", new Integer(i % 100)).create());
    outputCollector.emit(new TupleBuilderImpl().set("number", new Integer(i % 10)).create());
  }

  @Override
  public void configure(StreamsConfigurator streamsConfigurator) {
    TupleSchema inout = new TupleSchemaBuilderImpl().
        add("number", Integer.class).
        create();
    streamsConfigurator.getDefaultTupleInputStream().setSchema(inout);
    streamsConfigurator.getDefaultTupleOutputStream().setSchema(inout);
  }
}
