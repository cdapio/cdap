package com.continuuity.testsuite.purchaseanalytics;

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

/**
 *
 */
public class CustomerStoreFlowlet extends AbstractFlowlet {

  @UseDataSet("customers")
  private ObjectStore<Customer> store;

  @ProcessInput("outCustomer")
  public void process(Customer customer) throws OperationException {
    store.write(Bytes.toBytes(customer.getCustomerId()), customer);
  }
}
