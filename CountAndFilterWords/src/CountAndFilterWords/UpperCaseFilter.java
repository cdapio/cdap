package CountAndFilterWords;

import java.lang.Character;

import com.continuuity.api.flow.flowlet.*;
import com.continuuity.api.flow.flowlet.builders.*;

public class UpperCaseFilter extends AbstractComputeFlowlet {

  @Override
  public void configure(StreamsConfigurator configurator) {
    TupleSchema schema = new TupleSchemaBuilder().
        add("field", String.class).
        add("word", String.class).
        create();
    configurator.getDefaultTupleInputStream().setSchema(schema);
    configurator.getDefaultTupleOutputStream().setSchema(schema);
  }

  @Override
  public void process(Tuple tuple, TupleContext tupleContext, OutputCollector outputCollector) {
    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() + ": Received tuple " + tuple);

    String word = tuple.get("word");
    if (word == null) return;
    // filter words that are not upper-cased
    if (!Character.isUpperCase(word.charAt(0))) return;

    Tuple output = new TupleBuilder().
        set("word", word).
        create();

    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() + ": Emitting tuple " + output);

    outputCollector.add(output);
  }
}
