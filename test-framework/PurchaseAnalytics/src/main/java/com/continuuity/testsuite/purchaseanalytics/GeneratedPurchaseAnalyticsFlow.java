package com.continuuity.testsuite.purchaseanalytics;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

/**
 * Generates purchases, customers, products and inventory.
 */
public class GeneratedPurchaseAnalyticsFlow implements Flow {
  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("GeneratedPurchaseAnalyticsFlow")
      .setDescription("Generates purchases, customers, products and inventory.")
      .withFlowlets().add("generator", new TransactionGeneratorFlowlet())
      .add("generated-purchase-collector", new PurchaseStoreFlowlet())
      .add("generated-product-collector", new ProductStoreFlowlet())
      .add("generated-customer-collector", new CustomerStoreFlowlet())
      .connect()
      .from("generator").to("generated-purchase-collector")
      .from("generator").to("generated-product-collector")
      .from("generator").to("generated-customer-collector").build();
  }
}

