package CountAndFilterWords;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;

import java.util.HashMap;
import java.util.Map;

public class StreamSource extends AbstractFlowlet {
  private OutputEmitter<Map<String,String>> output;

  public StreamSource() {
    super("source");
  }

  public void process(StreamEvent event) {

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