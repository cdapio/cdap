package com.continuuity.testsuite.purchaseanalytics;

/**
 *
 */
public class Customer extends SerializedObject {
  private final int customerId;
  private final String name;
  private final short zip;
  private final short rating;

  public int getCustomerId() {
    return customerId;
  }

  public String getName() {
    return name;
  }

  public short getZip() {
    return zip;
  }

  public short getRating() {
    return rating;
  }

  public Customer(int customerId, String name, short zip, short rating) {
    super();
    this.customerId = customerId;
    this.name = name;
    this.zip = zip;
    this.rating = rating;
  }
}
