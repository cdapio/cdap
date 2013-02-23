package SimpleWriteAndRead;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecification;

public class ReaderFlowlet extends AbstractFlowlet {

  @UseDataSet(Common.tableName)
  KeyValueTable kvTable;

  public ReaderFlowlet() {
    super("reader");
  }

  public FlowletSpecification configure() {
    return FlowletSpecification.Builder.with()
      .setName(getName())
      .setDescription(getDescription())
      .useDataSet(Common.tableName)
      .build();
  }

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