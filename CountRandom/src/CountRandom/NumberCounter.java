package CountRandom;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.flow.flowlet.ComputeFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecifier;
import com.continuuity.api.flow.flowlet.OutputCollector;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.TupleContext;
import com.continuuity.api.flow.flowlet.TupleSchema;
import com.continuuity.api.flow.flowlet.builders.TupleSchemaBuilder;

public class NumberCounter extends ComputeFlowlet {

  static final byte[] column = { 'c', 'o', 'u', 'n', 't' };

  Table counters;

  @Override
  public void initialize() {
    //super.initialize();
    this.counters = this.getFlowletContext().getDataSet("counters");
  }

  @Override
  public void process(Tuple tuple, TupleContext tupleContext, OutputCollector outputCollector) {
    Integer i = tuple.get("number");
    getFlowletContext().getLogger().info("Processing integer " + i.intValue());
    try {
      counters.write(new Increment(i.toString().getBytes(), column, 1L));
    } catch (OperationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void configure(FlowletSpecifier specifier) {
    TupleSchema in = new TupleSchemaBuilder().
        add("number", Integer.class).
        create();
    specifier.getDefaultFlowletInput().setSchema(in);
  }

}
