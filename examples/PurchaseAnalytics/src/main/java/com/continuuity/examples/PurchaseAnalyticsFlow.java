package com.continuuity.examples;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

/**
 * Flow that get a purchase event and computes purchase analytics.
 */
public class PurchaseAnalyticsFlow implements Flow {

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("PurchaseAnalyticsFlow")
      .setDescription("Reads users purchase information and stores analytics in dataset")
      .withFlowlets()
      .add("reader", new PurchaseStreamReader())
      .add("collector", new PurchaseStore())
      .connect()
      .fromStream("purchaseEvent").to("reader")
      .from("reader").to("collector")
      .build();
  }
}
