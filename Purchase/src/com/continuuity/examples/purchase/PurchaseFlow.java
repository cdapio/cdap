
package com.continuuity.examples.purchase;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

/**
 * This is a simple flow that consumes purchase events from a stream and stores Purchase objects in datastore.
 */
public class PurchaseFlow implements Flow {


  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with().
      setName("PurchaseFlow").
      setDescription("Reads user and purchase stores in dataset").
      withFlowlets().
      add("reader", new PurchaseStreamReader()).
      add("store", new PurchaseStore()).
      connect().
      fromStream("purchaseStream").to("reader").
      from("reader").to("store").
      build();
  }
}
