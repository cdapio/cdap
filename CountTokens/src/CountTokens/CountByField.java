package CountTokens;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.flowlet.ComputeFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecifier;
import com.continuuity.api.flow.flowlet.OutputCollector;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.TupleContext;
import com.continuuity.api.flow.flowlet.TupleSchema;
import com.continuuity.api.flow.flowlet.builders.TupleSchemaBuilder;

public class CountByField extends ComputeFlowlet
{
  @Override
  public void configure(FlowletSpecifier specifier) {
    TupleSchema in = new TupleSchemaBuilder().
        add("field", String.class).
        add("word", String.class).
        create();
    specifier.getDefaultFlowletInput().setSchema(in);
  }

  KeyValueTable counters;

  @Override
  public void initialize() {
    this.counters = getFlowletContext().getDataSet(Common.tableName);
  }

  @Override
  public void process(Tuple tuple, TupleContext tupleContext, OutputCollector outputCollector) {
    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Received tuple " + tuple);
    }
    String token = tuple.get("word");
    if (token == null) {
      return;
    }
    String field = tuple.get("field");
    if (field != null) {
      token = field + ":" + token;
    }

    if (Common.debug) {
       System.out.println(this.getClass().getSimpleName() + ": Emitting Increment for " + token);
    }
    try {
      this.counters.increment(token.getBytes(), 1);
    } catch (OperationException e) {
      throw new RuntimeException(e);
    }
  }
}
