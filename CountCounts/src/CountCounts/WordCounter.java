package CountCounts;

import com.continuuity.api.flow.flowlet.*;
import com.continuuity.api.flow.flowlet.builders.*;

import java.lang.String;

public class WordCounter extends AbstractComputeFlowlet {

  @Override
  public void configure(StreamsConfigurator configurator) {
    TupleSchema in = new TupleSchemaBuilder().
        add("text", String.class).
        create();
    configurator.getDefaultTupleInputStream().setSchema(in);

    TupleSchema out = new TupleSchemaBuilder().
        add("count", Integer.class).
        create();
    configurator.getDefaultTupleOutputStream().setSchema(out);
  }

  @Override
  public void process(Tuple tuple, TupleContext tupleContext, OutputCollector outputCollector) {

    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() + ": Received tuple " + tuple);

    final String delimiters = "[ .-]";
    int count = 0;
    String str = (String)tuple.get("text");
    if (str != null) {
      count = str.split(delimiters).length;
    }

    Tuple output = new TupleBuilder().
        set("count", count).
        create();

    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() + ": Emitting tuple " + output);

    outputCollector.add(output);
  }

}
