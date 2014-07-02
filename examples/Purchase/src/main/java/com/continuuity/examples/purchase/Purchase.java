/**
 * Copyright 2013-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.continuuity.examples.purchase;

/**
 * This class represents a purchase made by a customer. It is a very simple class and only contains
 * the name of the customer, the name of the product, product quantity, price paid, and the purchase time.
 */
public class Purchase {

  private final String customer, product;
  private final int quantity, price;
  private final long purchaseTime;
  private String catalogId;

  public Purchase(String customer, String product, int quantity, int price, long purchaseTime) {
    this.customer = customer;
    this.product = product;
    this.quantity = quantity;
    this.price = price;
    this.purchaseTime = purchaseTime;
    this.catalogId = "";
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

  public String getCatalogId() {
    return catalogId;
  }

  public void setCatalogId(String catalogId) {
    this.catalogId = catalogId;
  }

}
