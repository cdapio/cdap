package CountTokens;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class StreamSource extends AbstractFlowlet {

  private OutputEmitter<Map<String,String>> output;


  public StreamSource() {
    super("source");
  }

  public void process(StreamEvent event) {

    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() + ": Received event " + event);

    ByteBuffer buf = event.getBody();
    byte[] body = buf.array();
    String text = body == null ? null :new String(body);

    Map<String, String> headers = event.getHeaders();
    String title = headers.get("title");

    Map<String,String> tuple = new HashMap<String,String>();
    tuple.put("title", title);
    tuple.put("text", text);

    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() + ": Emitting tuple " + tuple);

    output.emit(tuple);
  }
}