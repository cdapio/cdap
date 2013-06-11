
package com.continuuity.examples.purchase;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

/**
 * This is a simple flow that consumes purchase events from a stream and stores Purchase objects in datastore.
 * It has only two flowlets: one consumes events from the stream and converts them into Purchase objects,
 * the other consumes these objects and stores them in a dataset.
 */
public class PurchaseFlow implements Flow {


  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with().
      setName("PurchaseFlow").
      setDescription("Reads user and purchase information and stores in dataset").
      withFlowlets().
      add("reader", new PurchaseStreamReader()).
      add("collector", new PurchaseStore()).
      connect().
      fromStream("purchaseStream").to("reader").
      from("reader").to("collector").
      build();
  }
}
