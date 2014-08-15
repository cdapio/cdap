/*
 * Copyright 2014 Cask, Inc.
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
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Store the incoming Purchase objects in the purchases DataSet.
 */
public class PurchaseStore extends AbstractFlowlet {

  @UseDataSet("purchases")
  private ObjectStore<Purchase> store;
  private static final Logger LOG = LoggerFactory.getLogger(PurchaseStore.class);
  private ServiceDiscovered serviceDiscovered;

  @Override
  public void initialize(FlowletContext context) {
    //Discover the UserInterestsLookup service via discovery service
    // the service name and runnable are the same as the one provided in the Application configure method
    serviceDiscovered = context.discover("PurchaseHistory", PurchaseApp.SERVICE_NAME, PurchaseApp.SERVICE_NAME);
  }
  @ProcessInput
  public void process(Purchase purchase) {
    Discoverable discoverable = Iterables.getFirst(serviceDiscovered, null);
    if (discoverable != null) {
      // Look up user preference by calling the HTTP service that is started by UserPreferenceService.
      String hostName = discoverable.getSocketAddress().getHostName();
      int port = discoverable.getSocketAddress().getPort();
      String catalog = getCatalogId(hostName, port, purchase.getProduct());
      if (catalog != null) {
        purchase.setCatalogId(catalog);
      }
    }
    LOG.info("Purchase info: Customer {}, ProductId {}, CatalogId {}", purchase.getCustomer(),
                        purchase.getProduct(), purchase.getCatalogId());
    store.write(Bytes.toBytes(purchase.getPurchaseTime()), purchase);
  }

  /**
   * Make an HTTP call to the catalog service for a given product.
   * @return the catalog ID of the product, or null in case of error
   */
  private String getCatalogId(String host, int port, String productId) {
    try {
      URL url = new URL(String.format("http://%s:%d/v1/product/%s/catalog", host, port, productId));
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
