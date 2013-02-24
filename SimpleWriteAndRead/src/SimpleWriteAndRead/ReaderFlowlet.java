package SimpleWriteAndRead;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReaderFlowlet extends AbstractFlowlet {
  private static Logger LOG = LoggerFactory.getLogger(ReaderFlowlet.class);

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
    LOG.debug(this.getContext().getName() + ": Received key " + key);

    // perform inline read of key

    byte [] value = this.kvTable.read(key);

    if (value == null) {
      LOG.debug(this.getContext().getName() + ": No value read for key (" + new String(key) + ")");
    } else {
      LOG.debug(this.getContext().getName() + ": Read value (" + new String(value) +
            ") for key (" + new String(key) + ")");
    }
  }
}