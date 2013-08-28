package com.continuuity.testsuite.purchaseanalytics.datamodel;

/**
 *
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
    super();
    this.productId = productId;
    this.description = description;
  }
}
