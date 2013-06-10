package com.continuuity.examples.purchase;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

import java.util.ArrayList;
import java.util.List;

/**
 * Store the incoming Purchase Objects in datastore.
 */
public class PurchaseStore extends AbstractFlowlet {

  @UseDataSet("purchases")
  private ObjectStore<Purchase> store;

  public void process(Purchase purchase) throws OperationException {
    store.write(Bytes.toBytes(purchase.getPurchaseTime()), purchase);
  }
}
