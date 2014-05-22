package com.continuuity.examples.incrementbench;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

/**
 *
 */
public class IncrementFlow implements Flow {
  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("IncrementFlow")
      .setDescription("Generates random increments against a table")
      .withFlowlets()
      .add("generator", new IncrementGenerator())
      .add("consumer", new IncrementConsumer())
      .connect()
      .from("generator").to("consumer")
      .build();
  }
}
