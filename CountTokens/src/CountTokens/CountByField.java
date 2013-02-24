package CountTokens;

import com.continuuity.api.annotation.Process;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CountByField extends AbstractFlowlet {
  private static Logger LOG = LoggerFactory.getLogger(CountByField.class);

  public CountByField() {
    super("CountByField");
  }

  @UseDataSet("counters")
  KeyValueTable counters;

  public FlowletSpecification configure() {
    return FlowletSpecification.Builder.with()
      .setName(getName())
      .setDescription(getDescription())
      .useDataSet("counters")
      .build();
  }

  @Process("splitOut")
  public void process(Map<String, String> tupleIn) throws OperationException {
    LOG.debug(this.getContext().getName() + ": Received tuple " + tupleIn);

    String token = tupleIn.get("word");
    if (token == null) {
      return;
    }
    String field = tupleIn.get("field");
    if (field != null) {
      token = field + ":" + token;
    }

    LOG.debug(this.getContext().getName() + ": Emitting Increment for " + token);

    this.counters.increment(token.getBytes(), 1);
  }

  @Process("upperOut")
  public void process(String word) throws OperationException {
    LOG.debug(this.getContext().getName() + ": Received word " + word);


    if (word == null) {
      return;
    }

    LOG.debug(this.getContext().getName() + ": Emitting Increment for " + word);

    this.counters.increment(word.getBytes(), 1);
  }
}