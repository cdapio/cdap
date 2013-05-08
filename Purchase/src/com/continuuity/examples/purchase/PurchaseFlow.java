package com.continuuity.examples.purchase;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

/**
 *
 */
public class PurchaseFlow implements Flow {


  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with().
      setName("PurchaseHistoryFlow").
      setDescription("Aggregates the purchase history of each customer").
      withFlowlets().
      add("reader", new PurchaseStreamReader()).
      add("collector", new PurchaseCollector()).
      connect().
      fromStream("purchases").to("reader").
      from("reader").to("collector").
      build();
  }
}
