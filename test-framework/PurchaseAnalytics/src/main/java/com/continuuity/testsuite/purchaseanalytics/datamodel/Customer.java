package com.continuuity.testsuite.purchaseanalytics.datamodel;

import java.util.UUID;

/**
 * represents a customer
 */
public class Customer {
  private final long customerId;
  private final String name;
  private final int zip;
  private final int rating;

  public long getCustomerId() {
    return customerId;
  }

  public String getName() {
    return name;
  }

  public int getZip() {
    return zip;
  }

  public int getRating() {
    return rating;
  }

  public Customer(long customerId, String name, int zip, int rating) {
    //super();
    this.customerId = customerId;
    this.name = name;
    this.zip = zip;
    this.rating = rating;
  }
}
