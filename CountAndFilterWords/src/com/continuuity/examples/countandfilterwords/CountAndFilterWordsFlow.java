package com.continuuity.examples.countandfilterwords;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

public class CountAndFilterWordsFlow implements Flow {
  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("CountAndFilterWords")
      .setDescription("Flow for counting words")
      .withFlowlets()
        .add("source", new StreamSource())
        .add("splitter", new Tokenizer())
        .add("upper-filter", new DynamicFilterFlowlet("upper", "^[A-Z]+$"))
        .add("lower-filter", new DynamicFilterFlowlet("lower", "^[a-z]+$"))
        .add("number-filter", new DynamicFilterFlowlet("number", "^[0-9]+$"))
        .add("count-all", new Counter())
        .add("count-upper", new Counter())
        .add("count-lower", new Counter())
        .add("count-number", new Counter())
      .connect()
        .fromStream("text").to("source")
        .from("source").to("splitter")
        .from("splitter").to("count-all")
        .from("splitter").to("upper-filter")
        .from("splitter").to("lower-filter")
        .from("splitter").to("number-filter")
        .from("upper-filter").to("count-upper")
        .from("lower-filter").to("count-lower")
        .from("number-filter").to("count-number")
      .build();
  }
}
