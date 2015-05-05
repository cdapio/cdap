/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.examples.purchase;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.ObjectMappedTable;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.metrics.Metrics;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Store the incoming Purchase objects in the purchases DataSet.
 */
public class PurchaseStore extends AbstractFlowlet {

  @UseDataSet("purchases")
  private ObjectMappedTable<Purchase> store;
  private Metrics metrics; // Declare the custom metrics

  private static final Logger LOG = LoggerFactory.getLogger(PurchaseStore.class);

  @ProcessInput
  public void process(Purchase purchase) {
    // Discover the CatalogLookup service via discovery service
    // the service name is the same as the one provided in the Application configure method
    URL serviceURL = getContext().getServiceURL(PurchaseApp.APP_NAME, CatalogLookupService.SERVICE_NAME);
    if (serviceURL != null) {
      String catalog = getCatalogId(serviceURL, purchase.getProduct());
      if (catalog != null) {
        purchase.setCatalogId(catalog);
      }
    }
    metrics.count("purchases." + purchase.getCustomer(), 1);

    LOG.info("Purchase info: Customer {}, ProductId {}, CatalogId {}",
             purchase.getCustomer(), purchase.getProduct(), purchase.getCatalogId());
    store.write(Bytes.toBytes(purchase.getPurchaseTime()), purchase);
  }

  /**
   * Make an HTTP call to the catalog service for a given product.
   * @return the catalog ID of the product, or null in case of error
   */
  private String getCatalogId(URL baseURL, String productId) {
    try {
      URL url = new URL(baseURL, String.format("v1/product/%s/catalog", productId));
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      if (HttpURLConnection.HTTP_OK == conn.getResponseCode()) {
        try {
          return new String(ByteStreams.toByteArray(conn.getInputStream()), Charsets.UTF_8);
        } finally {
          conn.disconnect();
        }
      }
      LOG.warn("Unexpected response from Catalog Service: {} {}", conn.getResponseCode(), conn.getResponseMessage());
    } catch (Throwable th) {
      LOG.warn("Error while callilng Catalog Service", th);
    }
    return null;
  }

}
