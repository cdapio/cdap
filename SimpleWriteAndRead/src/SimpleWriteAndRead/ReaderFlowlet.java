package SimpleWriteAndRead;

import com.continuuity.api.data.*;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.flowlet.*;
import com.continuuity.api.flow.flowlet.builders.*;

import javax.management.OperationsException;

public class ReaderFlowlet extends ComputeFlowlet {

  @Override
  public void configure(FlowletSpecifier specifier) {
    TupleSchema in = new TupleSchemaBuilder().
        add("key", byte[].class).
        create();
    specifier.getDefaultFlowletInput().setSchema(in);
  }

  KeyValueTable kvTable;

  @Override
  public void initialize() {
    this.kvTable = getFlowletContext().getDataSet(Common.tableName);
  }

  @Override
  public void process(Tuple tuple, TupleContext tupleContext,
                      OutputCollector outputCollector) {
    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() +
          ": Received tuple " + tuple);

    // perform inline read of key
    byte [] key = tuple.get("key");
    try {
      byte [] value = this.kvTable.read(key);

      if (Common.debug) {
        if (value == null) {
          System.out.println(this.getClass().getSimpleName() +
              ": No value read for key (" + new String(key) + ")");
        } else {
          System.out.println(this.getClass().getSimpleName() +
              ": Read value (" + new String(value) +
              ") for key (" + new String(key) + ")");
        }
      }
    } catch (OperationException e) {
      System.err.println(this.getClass().getSimpleName() + ":  Error " +
          "reading value for key (" + new String(key) + "): " + e.getMessage());
    }
  }
}
