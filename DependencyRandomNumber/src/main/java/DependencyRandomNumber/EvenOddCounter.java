package DependencyRandomNumber;

import com.continuuity.api.data.*;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.flowlet.*;
import com.continuuity.api.flow.flowlet.builders.*;

public class EvenOddCounter extends ComputeFlowlet {

  @Override
  public void configure(FlowletSpecifier specifier) {
    TupleSchema in = new TupleSchemaBuilder().
        add("randomNumber", Long.class).
        create();
    specifier.getDefaultFlowletInput().setSchema(in);
  }

  KeyValueTable counters;
  static final byte[] keyEven = "even".getBytes();
  static final byte[] keyOdd = "odd".getBytes();

  @Override
  public void initialize() {
    this.counters = getFlowletContext().getDataSet(Common.tableName);
  }

  @Override
  public void process(Tuple tuple, TupleContext tupleContext,
      OutputCollector outputCollector) {
    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Received tuple " + tuple);
    }
    // count the number of odd or even numbers
    long randomNumber = ((Long)tuple.get("randomNumber")).longValue();
    boolean isEven = (randomNumber % 2) == 0;
    // determine the key for an increment operation
    byte[] key = isEven ? keyEven : keyOdd;
    // emit the increment operation
    this.counters.stage(new KeyValueTable.IncrementKey(key, 1));
  }
}
