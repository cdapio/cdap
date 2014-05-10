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
