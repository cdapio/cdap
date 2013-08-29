package com.continuuity.testsuite.purchaseanalytics.datamodel;

/**
 * Represents purchase stats for a given customer.
 */
public class PurchaseStat {
  private final long customerId;
  private final double averageSpending;
  private final long totalSpending;

  public long getCustomerId() {
    return customerId;
  }

  public double getAverageSpending() {
    return averageSpending;
  }

  public long getTotalSpending() {
    return totalSpending;
  }

  public PurchaseStat(long customerId, double averageSpending, long totalSpending) {
    this.customerId = customerId;
    this.averageSpending = averageSpending;
    this.totalSpending = totalSpending;
  }
}
