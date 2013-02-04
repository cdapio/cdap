package CountAndFilterWords;

import com.continuuity.api.flow.flowlet.*;
import com.continuuity.api.flow.flowlet.builders.*;

import java.util.HashMap;

public class StreamSource extends ComputeFlowlet {

  @Override
  public void configure(FlowletSpecifier specifier) {
    TupleSchema out = new TupleSchemaBuilder().
        add("title", String.class).
        add("text", String.class).
        create();
    specifier.getDefaultFlowletOutput().setSchema(out);
    specifier.getDefaultFlowletInput().setSchema(TupleSchema.EVENT_SCHEMA);
  }

  @Override
  public void process(Tuple tuple, TupleContext tupleContext, OutputCollector outputCollector) {

    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Received tuple " + tuple);
    }
    HashMap<String, String> headers = tuple.get("headers");
    byte[] body = tuple.get("body");
    String title = headers.get("title");
    String text = body == null ? null :new String(body);

    Tuple output = new TupleBuilder().
        set("title", title).
        set("text", text).
        create();

    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Emitting tuple " + output);
    }
    outputCollector.add(output);
  }
}
