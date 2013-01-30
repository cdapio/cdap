package SimpleWriteAndRead;

import com.continuuity.api.data.*;
import com.continuuity.api.flow.flowlet.*;
import com.continuuity.api.flow.flowlet.builders.*;

public class WriterFlowlet extends ComputeFlowlet {

  @Override
  public void configure(FlowletSpecifier specifier) {
    TupleSchema in = new TupleSchemaBuilder().
        add("title", String.class).
        add("text", String.class).
        create();
    specifier.getDefaultFlowletInput().setSchema(in);

    TupleSchema out = new TupleSchemaBuilder().
        add("key", byte[].class).
        create();
    specifier.getDefaultFlowletOutput().setSchema(out);
  }

  @Override
  public void process(Tuple tuple, TupleContext tupleContext,
                      OutputCollector outputCollector) {
    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() +
          ": Received tuple " + tuple);

    // text should be in the form: key=value
    String text = tuple.get("text");
    String [] params = text.split("=");
    if (params.length != 2) return;
    byte [] key = params[0].getBytes();
    byte [] value = params[1].getBytes();
    Write write = new Write(key, value);
    outputCollector.add(write);
    Tuple outputTuple = new TupleBuilder().
          set("key", key).
          create();
    outputCollector.add(outputTuple);
  }
}
