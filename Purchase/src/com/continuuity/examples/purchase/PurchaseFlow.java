package com.continuuity.examples.purchase;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

/**
 * This is a simple flow that consumes purchase events from a stream and builds each customer's purchase history.
 * It has only two flowlets: one consumes events from the stream and converts them into Purchase objects,
 * the other consumes these objects and aggregates the purchase history in an object store dataset.
 */
public class PurchaseFlow implements Flow {

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with().
      setName("PurchaseHistoryFlow").
      setDescription("Aggregates the purchase history of each customer").
      withFlowlets().
      add("reader", new PurchaseStreamReader()).
      add("collector", new PurchaseHistoryBuilder()).
      connect().
      fromStream("purchases").to("reader").
      from("reader").to("collector").
      build();
  }
}
