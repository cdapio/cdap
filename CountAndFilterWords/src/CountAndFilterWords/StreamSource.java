package CountAndFilterWords;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class StreamSource extends AbstractFlowlet {
  private static Logger LOG = LoggerFactory.getLogger(StreamSource.class);

  private OutputEmitter<Record> output;

  public StreamSource() {
    super("source");
  }

  public void process(StreamEvent event) {
    LOG.debug(this.getContext().getName() + ": Received event " + event);

    Map<String, String> headers = event.getHeaders();
    String title = headers.get("title");
    byte[] body = Bytes.toBytes(event.getBody());
    String text = body == null ? null :new String(body);

    Record record = new Record(title, text);

    LOG.debug(this.getContext().getName() + ": Emitting tuple " + output);

    output.emit(record);
  }
}