package com.continuuity.testsuite.purchaseanalytics.datamodel;

/**
 * Models a product identified by unique Id.
 */
public class Product {

  private final long productId;
  private final String description;

  public long getProductId() {
    return productId;
  }

  public  String getDescription() {
    return description;
  }

  public Product(long productId, String description) {
    this.productId = productId;
    this.description = description;
  }
}
