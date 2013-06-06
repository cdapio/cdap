package com.continuuity.examples.purchase;

/**
 * This represents a purchase made by a customer. It is a very simple class and only contains
 * the name of the customer, the name of the product, the quantity and the price paid.
 */
public class Purchase {

  private final String customer, product;
  private final int quantity, price;

  public Purchase(String customer, String product, int quantity, int price) {
    this.customer = customer;
    this.product = product;
    this.quantity = quantity;
    this.price = price;
  }

  public String getCustomer() {
    return customer;
  }

  public String getProduct() {
    return product;
  }

  public int getQuantity() {
    return quantity;
  }

  public int getPrice() {
    return price;
  }
}
