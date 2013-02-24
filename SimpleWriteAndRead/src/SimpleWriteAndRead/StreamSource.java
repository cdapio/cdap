package SimpleWriteAndRead;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class StreamSource extends AbstractFlowlet {
  private static Logger LOG = LoggerFactory.getLogger(StreamSource.class);

  private OutputEmitter<Map<String,String>> output;

  public StreamSource() {
    super("source");
  }

  public void process(StreamEvent event) {
    LOG.debug(this.getContext().getName() + ": Received event " + event);

    byte[] body = Bytes.toBytes(event.getBody());
    String text = body == null ? null :new String(body);

    Map<String, String> headers = event.getHeaders();
    String title = headers.get("title");

    Map<String,String> tuple = new HashMap<String,String>();
    tuple.put("title", title);
    tuple.put("text", text);

    LOG.debug(this.getContext().getName() + ": Emitting tuple " + tuple);

    output.emit(tuple);
  }
}