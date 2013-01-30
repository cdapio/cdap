package CountOddAndEven;

import com.continuuity.api.flow.flowlet.FlowletSpecifier;
import com.continuuity.api.flow.flowlet.OutputCollector;
import com.continuuity.api.flow.flowlet.SourceFlowlet;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.TupleSchema;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import com.continuuity.api.flow.flowlet.builders.TupleSchemaBuilder;

import java.util.Random;

/**
 * Random number generator.
 */
public class RandomNumberGenerator extends SourceFlowlet {
  Random random;
  long millis = 0;
  int direction = 1;

  @Override
  public void generate(OutputCollector outputCollector) {
    Tuple out = new TupleBuilder().set("number", new Integer(this.random.nextInt(10000))).create();
    try {
      Thread.sleep(millis);
      millis += direction;
      if(millis > 100 || millis < 1) {
        direction = direction * -1;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    outputCollector.add(out);
  }

  @Override
  public void configure(FlowletSpecifier specifier) {
    TupleSchema out = new TupleSchemaBuilder().add("number", Integer.class).create();
    specifier.getDefaultFlowletOutput().setSchema(out);
    this.random = new Random(System.currentTimeMillis());
  }
}
