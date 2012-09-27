package CountAndFilterWords;

import com.continuuity.api.flow.flowlet.*;
import com.continuuity.api.flow.flowlet.builders.*;

public class Tokenizer extends ComputeFlowlet {

  @Override
  public void configure(StreamsConfigurator configurator) {
    TupleSchema in = new TupleSchemaBuilder().
        add("title", String.class).
        add("text", String.class).
        create();
    configurator.getDefaultTupleInputStream().setSchema(in);

    TupleSchema out = new TupleSchemaBuilder().
        add("field", String.class).
        add("word", String.class).
        create();
    configurator.getDefaultTupleOutputStream().setSchema(out);
  }

  @Override
  public void process(Tuple tuple, TupleContext tupleContext, OutputCollector outputCollector) {
    final String[] fields = { "title", "text" };

    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() + ": Received tuple " + tuple);

    for (String field : fields)
      tokenize((String)tuple.get(field), field, outputCollector);
  }

  void tokenize(String str, String field, OutputCollector outputCollector) {
    if (str == null) return;
    final String delimiters = "[ .-]";
    String[] tokens = str.split(delimiters);

    for (String token : tokens) {

      Tuple output = new TupleBuilder().
          set("field", field).
          set("word", token).
          create();

      if (Common.debug)
        System.out.println(this.getClass().getSimpleName() + ": Emitting tuple " + output);

      outputCollector.add(output);
    }
  }

}
