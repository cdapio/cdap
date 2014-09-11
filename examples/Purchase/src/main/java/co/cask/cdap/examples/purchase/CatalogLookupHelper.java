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


import co.cask.cdap.api.data.DataSetContext;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.service.AbstractServiceWorker;
import co.cask.cdap.api.service.TxRunnable;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

/**
 * Example ServiceWorker which upon start-up, iterates through the purchases dataset, and sets the catalogId for any
 * purchases for which the catalogID is invalid (null or empty string). This attribute may be invalid for a purchase,
 * for instance, if the CatalogLookup service was down, while a purchased arrived in the purchaseStream.
 */
public final class CatalogLookupHelper extends AbstractServiceWorker {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogLookupHelper.class);
  private int numUpdates = 0;

  private void updateCatalogIDs() {
    getContext().execute(new TxRunnable() {
      @Override
      public void run(DataSetContext context) throws Exception {
        ObjectStore<Purchase> table = context.getDataSet("purchases");
        List<Split> splits = table.getSplits();
        for (Split split : splits) {
          SplitReader<byte[], Purchase> reader = table.createSplitReader(split);
          reader.initialize(split);
          while (reader.nextKeyValue()) {
            byte[] key = reader.getCurrentKey();
            Purchase purchase = reader.getCurrentValue();
            String catalogId = purchase.getCatalogId();
            if (catalogId == null || catalogId.length() == 0) {
              LOG.info("catalogId was missing for item: " + purchase.getProduct());
              String newCatalogId = getCatalogId(purchase.getProduct());
              purchase.setCatalogId(newCatalogId);
              table.write(key, purchase);
              numUpdates++;
            }
          }
        }
      }
    });
  }

  @Override
  public void run() {
    updateCatalogIDs();
    LOG.info("Done updating the dataset. Performed {} updates.", numUpdates);
  }

  /**
   * Make an HTTP call to the catalog service for a given product.
   * @return the catalog ID of the product, or null in case of error
   */
  private String getCatalogId(String productId) {
    URL serviceURL = getContext().getServiceURL(PurchaseApp.SERVICE_NAME);
    if (serviceURL == null) {
      return null;
    }

    try {
      URL url = new URL(serviceURL, String.format("v1/product/%s/catalog", productId));
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
      LOG.warn("Error while callling Catalog Service", th);
    }
    return null;
  }
}
