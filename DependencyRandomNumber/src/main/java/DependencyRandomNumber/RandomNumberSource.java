package DependencyRandomNumber;

import java.util.Random;

import com.continuuity.api.flow.flowlet.AbstractSourceFlowlet;
import com.continuuity.api.flow.flowlet.FlowletLaunchContext;
import com.continuuity.api.flow.flowlet.OutputCollector;
import com.continuuity.api.flow.flowlet.StreamsConfigurator;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.TupleSchema;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import com.continuuity.api.flow.flowlet.builders.TupleSchemaBuilder;

public class RandomNumberSource extends AbstractSourceFlowlet {
  
  private Random random;

  @Override
  public void configure(StreamsConfigurator configurator) {
    TupleSchema out = new TupleSchemaBuilder().
        add("randomNumber", Long.class).
        create();
    configurator.getDefaultTupleOutputStream().setSchema(out);
  }

  @Override
  public void initialize(FlowletLaunchContext launchContext) {
    super.initialize(launchContext);
    this.random = new Random();
  }
  
  @Override
  public void generate(OutputCollector outputCollector) {
    long randomNumber = Math.abs(this.random.nextLong());
    Tuple randomNumberTuple = new TupleBuilder()
        .set("randomNumber", randomNumber)
        .create();
    outputCollector.emit(randomNumberTuple);
  }
}
