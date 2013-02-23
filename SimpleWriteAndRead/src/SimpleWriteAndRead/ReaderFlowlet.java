package SimpleWriteAndRead;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

public class ReaderFlowlet extends AbstractFlowlet {

  public ReaderFlowlet() {
    super("reader");
  }

  KeyValueTable kvTable;


  public void process(byte[] key) throws OperationException {
    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() + ": Received key " + key);

    // perform inline read of key

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
  }
}