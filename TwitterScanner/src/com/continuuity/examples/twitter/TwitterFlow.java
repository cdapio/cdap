/*
 * twitterScanner - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.examples.twitter;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

public class TwitterFlow implements Flow {
  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("TwitterScanner")
      .setDescription("Twitter Demo")
      .withFlowlets()
        .add("Reader", new TwitterGenerator())
        .add("Processor", new TwitterProcessor())
      .connect()
        .from("Reader").to("Processor")
      .build();
  }
}
