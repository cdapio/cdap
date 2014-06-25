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

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.lib.ObjectStore;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

/**
 * Store the incoming Purchase objects in the purchases DataSet.
 */
public class PurchaseStore extends AbstractFlowlet {

  @UseDataSet("purchases")
  private ObjectStore<Purchase> store;

  @ProcessInput
  public void process(Purchase purchase) {
    store.write(Bytes.toBytes(purchase.getPurchaseTime()), purchase);
  }
}
