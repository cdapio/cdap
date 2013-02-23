package CountRandom;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecification;

public class NumberCounter extends AbstractFlowlet {
  static final byte[] column = { 'c', 'o', 'u', 'n', 't' };

  @UseDataSet("counters")
  Table counters;

  public NumberCounter() {
    super("count");
  }

  public void process(Integer number) {
    try {
      counters.write(new Increment(number.toString().getBytes(), column, 1L));
    } catch (OperationException e) {
      throw new RuntimeException(e);
    }
  }

}
