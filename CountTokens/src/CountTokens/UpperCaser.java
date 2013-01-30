package CountTokens;

import com.continuuity.api.flow.flowlet.*;
import com.continuuity.api.flow.flowlet.builders.*;

public class UpperCaser extends ComputeFlowlet {

  @Override
  public void configure(FlowletSpecifier specifier) {
    TupleSchema schema = new TupleSchemaBuilder().
        add("field", String.class).
        add("word", String.class).
        create();
    specifier.getDefaultFlowletInput().setSchema(schema);
    specifier.getDefaultFlowletOutput().setSchema(schema);
  }

  @Override
  public void process(Tuple tuple, TupleContext tupleContext, OutputCollector outputCollector) {
    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() + ": Received tuple " + tuple);

    String word = tuple.get("word");
    if (word == null) return;
    String upper = word.toUpperCase();

    Tuple output = new TupleBuilder().
        set("word", upper).
        create();

    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() + ": Emitting tuple " + output);

    outputCollector.add(output);
  }
}
