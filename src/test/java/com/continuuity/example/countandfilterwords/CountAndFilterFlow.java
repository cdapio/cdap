package com.continuuity.example.countandfilterwords;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

public class CountAndFilterFlow implements Flow {
  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("CountAndFilterFlow")
      .setDescription("Flow for counting words")
      .withFlowlets()
        .add("source", new StreamSource(), 1)
        .add("split-words", new Tokenizer(), 1)
        .add("upper-filter", new UpperCaseFilter(), 1)
        .add("count-all", new CountByField(), 1)
        .add("count-upper", new CountByField(), 1)
      .connect()
        .fromStream("text").to("source")
        .from("source").to("split-words")
        .from("split-words").to("count-all")
        .from("split-words").to("upper-filter")
        .from("upper-filter").to("count-upper")
      .build();
  }
}
