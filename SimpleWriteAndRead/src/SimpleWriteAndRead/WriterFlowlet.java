package SimpleWriteAndRead;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;

import java.util.Map;

public class WriterFlowlet extends AbstractFlowlet {

  public WriterFlowlet() {
    super("writer");
  }

  KeyValueTable kvTable;

  private OutputEmitter<byte[]> output;

  public void process(Map<String, String> tupleIn) throws OperationException {
    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() + ": Received tuple " + tupleIn);

    // text should be in the form: key=value
    String text = tupleIn.get("text");
    String [] params = text.split("=");
    if (params.length != 2) return;
    byte [] key = params[0].getBytes();
    byte [] value = params[1].getBytes();

    this.kvTable.write(key, value);

    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() + ": Emitting key " + key);

    output.emit(key);
  }
}
