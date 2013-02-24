package SimpleWriteAndRead;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class WriterFlowlet extends AbstractFlowlet {
  private static Logger LOG = LoggerFactory.getLogger(WriterFlowlet.class);

  @UseDataSet(Common.tableName)
  KeyValueTable kvTable;

  private OutputEmitter<byte[]> output;

  public WriterFlowlet() {
    super("writer");
  }

  public FlowletSpecification configure() {
    return FlowletSpecification.Builder.with()
      .setName(getName())
      .setDescription(getDescription())
      .useDataSet(Common.tableName)
      .build();
  }

  public void process(Map<String, String> tupleIn) throws OperationException {
    LOG.debug(this.getContext().getName() + ": Received tuple " + tupleIn);

    // text should be in the form: key=value
    String text = tupleIn.get("text");
    String [] params = text.split("=");
    if (params.length != 2) return;
    byte [] key = params[0].getBytes();
    byte [] value = params[1].getBytes();

    this.kvTable.write(key, value);

    LOG.debug(this.getContext().getName() + ": Emitting key " + key);

    output.emit(key);
  }
}
