/*
 * Copyright 2014 Cask Data, Inc.
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
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.AbstractServiceWorker;
import co.cask.cdap.api.service.TxRunnable;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * A Catalog Lookup Service implementation that provides ids for products.
 */
public class CatalogLookupService extends AbstractService {

  @Override
  protected void configure() {
    setName(PurchaseApp.SERVICE_NAME);
    setDescription("Service to lookup product ids.");
    addHandler(new ProductCatalogLookup());
    addWorker(new CatalogLookupHelper());
    useDataset("purchases");
  }

  /**
   * Example ServiceWorker which upon start-up, iterates through the purchases dataset, and sets the catalogId for any
   * purchases for which the catalogID is invalid (null or empty string). This attribute may be invalid for a purchase,
   * for instance, if the CatalogLookup service was down, while a purchased arrived in the purchaseStream.
   */
  public static final class CatalogLookupHelper extends AbstractServiceWorker {
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
                purchase.setCatalogId("Catalog-" + purchase.getProduct());
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
  }

  /**
   * Lookup Handler to serve requests.
   */
  @Path("/v1")
  public static final class ProductCatalogLookup extends AbstractHttpServiceHandler {

    @Path("product/{id}/catalog")
    @GET
    public void handler(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("id") String id) {
      // send string Catalog-<id> with 200 OK response.
      responder.sendString(200, "Catalog-" + id, Charsets.UTF_8);
    }
  }
}
