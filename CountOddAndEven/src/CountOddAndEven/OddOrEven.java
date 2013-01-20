package CountOddAndEven;

import com.continuuity.api.flow.flowlet.ComputeFlowlet;
import com.continuuity.api.flow.flowlet.OutputCollector;
import com.continuuity.api.flow.flowlet.StreamsConfigurator;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.TupleContext;
import com.continuuity.api.flow.flowlet.TupleSchema;
import com.continuuity.api.flow.flowlet.builders.TupleSchemaBuilder;

/**
 * Based on the whether number is odd or even it puts the number on
 * different streams.
 */
public class OddOrEven extends ComputeFlowlet {

  @Override
  public void configure(final StreamsConfigurator configurator) {
    TupleSchema schema = new TupleSchemaBuilder().add("number", Integer.class).create();
    configurator.getDefaultTupleInputStream().setSchema(schema);
    configurator.addTupleInputStream("odd").setSchema(schema);
    configurator.addTupleOutputStream("even").setSchema(schema);
  }

  @Override
  public void process(final Tuple tuple, final TupleContext context, final OutputCollector collector) {
    Integer num = tuple.get("number");
    if(num.intValue() % 2 == 0) {
      collector.add("even", tuple);
    } else {
      collector.add("odd", tuple);
    }
  }
}
