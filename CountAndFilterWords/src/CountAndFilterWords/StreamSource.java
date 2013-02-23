package CountAndFilterWords;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;

import java.nio.charset.CharacterCodingException;
import java.util.HashMap;
import java.util.Map;

public class StreamSource extends AbstractFlowlet {
  private OutputEmitter<Map<String,String>> output;

  public StreamSource() {
    super("StreamSource");
  }

//  @Override
//  public void configure(FlowletSpecifier specifier) {
//    TupleSchema out = new TupleSchemaBuilder().
//        add("title", String.class).
//        add("text", String.class).
//        create();
//    specifier.getDefaultFlowletOutput().setSchema(out);
//    specifier.getDefaultFlowletInput().setSchema(TupleSchema.EVENT_SCHEMA);
//  }

  public void process(StreamEvent event) throws CharacterCodingException {

    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Received event " + event);
    }

    Map<String, String> headers = event.getHeaders();
    String title = headers.get("title");
    byte[] body = event.getBody().array();
    String text = body == null ? null :new String(body);

    Map<String,String> tuple = new HashMap<String,String>();
    tuple.put("title", title);
    tuple.put("text", text);

    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Emitting tuple " + output);
    }
    output.emit(tuple);
  }
}