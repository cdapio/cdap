package com.continuuity.examples;

/**
 * This class represents a purchase made by a customer. It is a very simple class and only contains
 * the name of the customer, the name of the product, product quantity, price paid, and the purchase time.
 */
public class Purchase {

  private final String customer, product;
  private final int quantity, price;
  private final long purchaseTime;

  public Purchase(String customer, String product, int quantity, int price, long purchaseTime) {
    this.customer = customer;
    this.product = product;
    this.quantity = quantity;
    this.price = price;
    this.purchaseTime = purchaseTime;
  }

  public String getCustomer() {
    return customer;
  }

  public String getProduct() {
    return product;
  }

  public long getPurchaseTime() {
    return purchaseTime;
  }

  public int getQuantity() {
    return quantity;
  }

  public int getPrice() {
    return price;
  }
}
