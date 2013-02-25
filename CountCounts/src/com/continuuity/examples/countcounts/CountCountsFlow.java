package com.continuuity.examples.countcounts;


import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

public class CountCountsFlow implements Flow {

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("CountCounts")
      .setDescription("Flow for counting words")
      .withFlowlets()
        .add("source", new StreamSource())
        .add("counter", new WordCounter())
        .add("incrementer", new Incrementer())
      .connect()
        .fromStream("text").to("source")
        .from("source").to("counter")
        .from("counter").to("incrementer")
      .build();
  }
}