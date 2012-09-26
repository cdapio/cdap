package CountRandom;

import com.continuuity.api.flow.flowlet.*;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import com.continuuity.api.flow.flowlet.builders.TupleSchemaBuilder;

import java.util.Random;

public class RandomSource extends AbstractSourceFlowlet {

  Random random;

  @Override
  public void generate(OutputCollector outputCollector) {
    Tuple out = new TupleBuilder().set("number", new Integer(this.random.nextInt(10000))).create();
    outputCollector.add(out);
  }

  @Override
  public void configure(StreamsConfigurator streamsConfigurator) {
    TupleSchema out = new TupleSchemaBuilder().
        add("number", Integer.class).
        create();
    streamsConfigurator.getDefaultTupleOutputStream().setSchema(out);
    this.random = new Random(System.currentTimeMillis());
  }
}
