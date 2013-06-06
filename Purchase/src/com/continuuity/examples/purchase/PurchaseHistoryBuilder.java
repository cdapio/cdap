package com.continuuity.examples.purchase;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

/**
 * A flowlet that builds customer's purchase histories.
 */
public class PurchaseHistoryBuilder extends AbstractFlowlet {

  @UseDataSet("history")
  private ObjectStore<PurchaseHistory> store;

  /**
   * For a given purchase, looks up the history of the customer and adds the new purchase,
   * then writes back the history to the store. If the customer does not have a history yet,
   * a new purchase history is created.
   * @param purchase the new purchase
   * @throws OperationException
   */
  public void process(Purchase purchase) throws OperationException {
    String customer = purchase.getCustomer();
    PurchaseHistory history = store.read(customer.getBytes());
    if (history == null) {
      history = new PurchaseHistory(purchase.getCustomer());
    }
    history.add(purchase);
    store.write(Bytes.toBytes(customer), history);
  }
}
