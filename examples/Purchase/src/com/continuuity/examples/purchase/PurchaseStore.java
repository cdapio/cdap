package com.continuuity.examples.purchase;

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

/**
 * Store the incoming Purchase objects in the purchases dataset.
 */
public class PurchaseStore extends AbstractFlowlet {

  @UseDataSet("purchases")
  private ObjectStore<Purchase> store;

  @ProcessInput
  public void process(Purchase purchase) {
    store.write(Bytes.toBytes(purchase.getPurchaseTime()), purchase);
  }
}
