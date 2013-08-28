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
public class ProductStoreFlowlet extends AbstractFlowlet {

  @UseDataSet("products")
  private ObjectStore<Product> store;

  @ProcessInput("outProduct")
  public void process(Product product) throws OperationException {
    store.write(Bytes.toBytes(product.getProductId()), product);
  }
}
