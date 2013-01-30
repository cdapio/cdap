package CountTokens;

import com.continuuity.api.data.Increment;
import com.continuuity.api.flow.flowlet.*;
import com.continuuity.api.flow.flowlet.builders.*;

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
    outputCollector.add(new Increment(token.getBytes(), 1));
  }
}
