/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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
