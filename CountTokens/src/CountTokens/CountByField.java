package counttokens;

import com.continuuity.api.data.Increment;
import com.continuuity.api.flow.flowlet.AbstractComputeFlowlet;
import com.continuuity.api.flow.flowlet.OutputCollector;
import com.continuuity.api.flow.flowlet.StreamsConfigurator;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.TupleContext;
import com.continuuity.api.flow.flowlet.TupleSchema;
import com.continuuity.flow.flowlet.internal.TupleSchemaBuilderImpl;

public class CountByField extends AbstractComputeFlowlet
{
  @Override
  public void configure(StreamsConfigurator configurator) {
    TupleSchema in = new TupleSchemaBuilderImpl().
        add("field", String.class).
        add("word", String.class).
        create();
    configurator.getDefaultTupleInputStream().setSchema(in);
  }

  @Override
  public void process(Tuple tuple, TupleContext tupleContext, OutputCollector outputCollector) {
    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() + ": Received tuple " + tuple);

    String token = tuple.get("word");
    if (token == null) return;
    String field = tuple.get("field");
    if (field != null) token = field + ":" + token;

    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() + ": Emitting Increment for " + token);
    outputCollector.emit(new Increment(token.getBytes(), 1));
  }
}
