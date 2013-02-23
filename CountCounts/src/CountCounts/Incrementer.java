package CountCounts;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecification;

public class Incrementer extends AbstractFlowlet {
  static String keyTotal = ":sinkTotal:";

  @UseDataSet(Common.tableName)
  CounterTable counters;

  public Incrementer() {
    super("tick");
  }

  public FlowletSpecification configure() {
    return FlowletSpecification.Builder.with()
      .setName("text")
      .setDescription("")
      .useDataSet(Common.tableName)
      .build();
  }


  public void process(Integer count) {
    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Received event " + count);
    }

    if (count == null) {
      return;
    }
    String key = Integer.toString(count);
    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Emitting Increment for " + key);
    }
    // emit an increment for the number of words in this document
    this.counters.increment(key);

    if (Common.count) {
      // emit an increment for the total number of documents counted
      this.counters.increment(keyTotal);
    }
  }
}
