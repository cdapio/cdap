package com.continuuity.examples.purchase;

public class Purchase {

  private final String who, product;
  private final int quantity, price;

  public Purchase(String who, String product, int quantity, int price) {
    this.who = who;
    this.product = product;
    this.quantity = quantity;
    this.price = price;
  }

  public String getWho() {
    return who;
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
