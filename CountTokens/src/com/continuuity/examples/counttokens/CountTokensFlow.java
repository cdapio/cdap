package com.continuuity.examples.counttokens;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

public class CountTokensFlow implements Flow {
  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("CountTokens")
      .setDescription("Example count token flow")
      .withFlowlets()
        .add("source", new StreamSource())
        .add("split", new Tokenizer())
        .add("count", new TokenCounter())
      .connect()
        .fromStream("text").to("source")
        .from("source").to("split")
        .from("split").to("count")
      .build();
  }
}
