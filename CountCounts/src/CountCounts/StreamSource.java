package CountCounts;


import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class StreamSource extends AbstractFlowlet {
  private static Logger LOG = LoggerFactory.getLogger(StreamSource.class);

  static String keyTotal = ":sourceTotal:";

  private OutputEmitter<String> output;

  @UseDataSet(Common.tableName)
  CounterTable counters;

  public FlowletSpecification configure() {
    return FlowletSpecification.Builder.with()
      .setName("text")
      .setDescription("")
      .useDataSet(Common.tableName)
      .build();
  }

  public StreamSource() {
    super("source");
  }

  public void process(StreamEvent event) {
    LOG.debug(this.getContext().getName() + ": Received event " + event);

    ByteBuffer buf = event.getBody();
    byte[] body = Bytes.toBytes(event.getBody());
    String text = body == null ? null :new String(body);

    LOG.debug(this.getContext().getName() + ": Emitting " + text);

    if (Common.count) {
      // emit an increment for the total number of documents ingested
      this.counters.increment(keyTotal);
    }

    output.emit(text);
  }
}