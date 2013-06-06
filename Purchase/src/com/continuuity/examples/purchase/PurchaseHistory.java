package com.continuuity.examples.purchase;

import java.util.ArrayList;
import java.util.List;

/**
 * This class represents the purchase history for one customer.
 */
public class PurchaseHistory {

  private final String customer;
  private final List<Purchase> purchases;

  public PurchaseHistory(String customer) {
    this.customer = customer;
    this.purchases = new ArrayList<Purchase>();
  }

  public String getCustomer() {
    return customer;
  }

  public List<Purchase> getPurchases() {
    return purchases;
  }

  /**
   * Add a purchase to a customer's history.
   * @param purchase the purchase
   */
  public void add(Purchase purchase) {
    this.purchases.add(purchase);
  }
}
