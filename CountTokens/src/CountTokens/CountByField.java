package CountTokens;

import com.continuuity.api.annotation.Process;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecification;

import java.util.Map;

public class CountByField extends AbstractFlowlet {

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
    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Received tuple " + tupleIn);
    }
    String token = tupleIn.get("word");
    if (token == null) {
      return;
    }
    String field = tupleIn.get("field");
    if (field != null) {
      token = field + ":" + token;
    }

    if (Common.debug) {
       System.out.println(this.getClass().getSimpleName() + ": Emitting Increment for " + token);
    }
      this.counters.increment(token.getBytes(), 1);
  }

  @Process("upperOut")
  public void process(String word) throws OperationException {
    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Received word " + word);
    }

    if (word == null) {
      return;
    }

    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Emitting Increment for " + word);
    }
    this.counters.increment(word.getBytes(), 1);
  }
}