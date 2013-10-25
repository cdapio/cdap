package com.continuuity.flows;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

/**
 * Log event processor.
 */
public class EventProcessor implements Flow {

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
                            .setName("EventProcessor")
                            .setDescription("Event processing flow")
                            .withFlowlets()
                              .add("normalizer", new Normalizer())
                              .add("aggregator", new Aggregator())
                              .add("persister", new Persister())
                            .connect()
                               .fromStream("event-stream")
                                .to("normalizer")
                               .from("normalizer")
                                .to("aggregator")
                               .from("normalizer")
                                .to("persister")
                            .build();
  }
}
