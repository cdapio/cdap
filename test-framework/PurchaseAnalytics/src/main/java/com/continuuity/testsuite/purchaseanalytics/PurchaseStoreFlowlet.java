package com.continuuity.testsuite.purchaseanalytics;

import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.testsuite.purchaseanalytics.Purchase;



/**
 * Store the incoming Purchase Objects in datastore.
 */
public class PurchaseStoreFlowlet extends AbstractFlowlet {

  @UseDataSet("purchases")
  private ObjectStore<Purchase> store;

  @ProcessInput("outPurchase")
  public void process(Purchase purchase) throws OperationException {
    store.write(Bytes.toBytes(purchase.getPurchaseTime()), purchase);
  }
}
