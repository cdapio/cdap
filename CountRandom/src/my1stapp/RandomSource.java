package my1stapp;

import com.continuuity.api.flow.flowlet.*;
import com.continuuity.flow.flowlet.internal.TupleBuilderImpl;
import com.continuuity.flow.flowlet.internal.TupleSchemaBuilderImpl;

import java.util.Random;

public class RandomSource extends AbstractSourceFlowlet {

  Random random;

  @Override
  public void generate(OutputCollector outputCollector) {
    Tuple out = new TupleBuilderImpl().set("number", new Integer(this.random.nextInt(10000))).create();
    outputCollector.emit(out);
  }

  @Override
  public void configure(StreamsConfigurator streamsConfigurator) {
    TupleSchema out = new TupleSchemaBuilderImpl().
        add("number", Integer.class).
        create();
    streamsConfigurator.getDefaultTupleOutputStream().setSchema(out);
    this.random = new Random(System.currentTimeMillis());
  }
}
