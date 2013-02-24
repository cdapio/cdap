package com.continuuity.examples.countcounts;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Incrementer extends AbstractFlowlet {
  private static Logger LOG = LoggerFactory.getLogger(Incrementer.class);

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
    LOG.debug(this.getContext().getName() + ": Received event " + count);

    if (count == null) {
      return;
    }
    String key = Integer.toString(count);

    LOG.debug(this.getContext().getName()  + ": Emitting Increment for " + key);

    // emit an increment for the number of words in this document
    this.counters.increment(key);

    if (Common.count) {
      // emit an increment for the total number of documents counted
      this.counters.increment(keyTotal);
    }
  }
}
