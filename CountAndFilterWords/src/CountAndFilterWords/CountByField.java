package CountAndFilterWords;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountByField extends AbstractFlowlet {

  private static Logger LOG = LoggerFactory.getLogger(CountByField.class);

  @UseDataSet(Common.counterTableName)
  KeyValueTable counters;

  public CountByField() {
    super("countByField");
  }

  public void process(Record record) throws OperationException {
    LOG.debug(this.getContext().getName() + ": Received record with word "
                + record.getWord() + " and field " + record.getField());

    String token = record.getWord();
    if (token == null) return;

    String field = record.getField();
    if (field != null) token = field + ":" + token;

    LOG.debug(this.getContext().getName() + ": Incrementing for token " + token);

    this.counters.increment(token.getBytes(), 1L);
  }
}
