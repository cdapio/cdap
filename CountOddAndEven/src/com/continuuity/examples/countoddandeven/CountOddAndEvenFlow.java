package com.continuuity.examples.countoddandeven;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

public class CountOddAndEvenFlow implements Flow {
  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("CountOddAndEven")
      .setDescription("Flow for counting words")
      .withFlowlets()
        .add("NumGenerator", new RandomNumberGenerator())
        .add("OddOrEven", new OddOrEven())
        .add("EvenCounter", new EvenCounter())
        .add("OddCounter", new OddCounter())
      .connect()
        .from("NumGenerator").to("OddOrEven")
        .from("OddOrEven").to("EvenCounter")
        .from("OddOrEven").to("OddCounter")
      .build();
  }
}
