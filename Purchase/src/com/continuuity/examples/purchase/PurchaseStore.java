package com.continuuity.examples.purchase;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class PurchaseStore extends AbstractFlowlet {

  @UseDataSet("purchases")
  private ObjectStore<List<Purchase>> store;

  public void process(Purchase purchase) throws OperationException {
    String customer = purchase.getCustomer();
    List<Purchase> purchases = store.read(customer.getBytes());
    if (purchases == null) {
      purchases = new ArrayList<Purchase>();
    }
    purchases.add(purchase);
    store.write(Bytes.toBytes(customer), purchases);
  }
}
