package CountCounts;

import com.continuuity.api.flow.flowlet.ComputeFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecifier;
import com.continuuity.api.flow.flowlet.OutputCollector;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.TupleContext;
import com.continuuity.api.flow.flowlet.TupleSchema;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import com.continuuity.api.flow.flowlet.builders.TupleSchemaBuilder;

import java.lang.String;

public class WordCounter extends ComputeFlowlet {

  @Override
  public void configure(FlowletSpecifier specifier) {
    TupleSchema in = new TupleSchemaBuilder().
        add("text", String.class).
        create();
    specifier.getDefaultFlowletInput().setSchema(in);

    TupleSchema out = new TupleSchemaBuilder().
        add("count", Integer.class).
        create();
    specifier.getDefaultFlowletOutput().setSchema(out);
  }

  @Override
  public void process(Tuple tuple, TupleContext tupleContext, OutputCollector outputCollector) {

    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Received tuple " + tuple);
    }
    final String delimiters = "[ .-]";
    int count = 0;
    String str = (String)tuple.get("text");
    if (str != null) {
      count = str.split(delimiters).length;
    }

    Tuple output = new TupleBuilder().
        set("count", count).
        create();

    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Emitting tuple " + output);
    }
    outputCollector.add(output);
  }

}
