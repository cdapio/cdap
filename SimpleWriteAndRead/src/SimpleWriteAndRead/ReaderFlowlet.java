package SimpleWriteAndRead;

import com.continuuity.api.data.*;
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

  @Override
  public void process(Tuple tuple, TupleContext tupleContext,
                      OutputCollector outputCollector) {
    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() +
          ": Received tuple " + tuple);

    // perform inline read of key
    byte [] key = tuple.get("key");
    ReadKey read = new ReadKey(key);
    DataFabric fabric =
      getFlowletContext().getDataFabric();
    try {
      OperationResult<byte []> value = fabric.read(read);

      if (Common.debug) {
        if (value.isEmpty()) {
          System.out.println(this.getClass().getSimpleName() +
              ": No value read for key (" + new String(key) + ")");
        } else {
          System.out.println(this.getClass().getSimpleName() +
              ": Read value (" + new String(value.getValue()) +
              ") for key (" + new String(key) + ")");
        }
      }
    } catch (OperationException e) {
      System.err.println(this.getClass().getSimpleName() + ":  Error " +
          "reading value for key (" + new String(key) + "): " + e.getMessage());
    }
  }
}
