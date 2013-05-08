package com.continuuity.examples.purchase;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

import java.util.ArrayList;

/**
 *
 */
public class PurchaseCollector extends AbstractFlowlet {

  @UseDataSet("purchases")
  private ObjectStore<ArrayList<Purchase>> store;

  public void process(Purchase purchase) throws OperationException {
    String buyer = purchase.getWho();
    ArrayList<Purchase> history = store.read(buyer.getBytes());
    if (history == null) {
      history = new ArrayList<Purchase>();
    }
    history.add(purchase);
    store.write(buyer.getBytes(), history);
  }
}
