package com.continuuity.testsuite.purchaseanalytics;

/**
 *
 */
public class Product extends SerializedObject {

  private final int productId;
  private final String description;

  public int getProductId() {
    return productId;
  }

  public  String getDescription() {
    return description;
  }

  public Product(int productId, String description) {
    super();
    this.productId = productId;
    this.description = description;
  }
}
