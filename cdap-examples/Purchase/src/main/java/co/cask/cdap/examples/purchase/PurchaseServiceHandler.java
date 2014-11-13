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

import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.net.HttpURLConnection;
import java.net.URL;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Purchase service handler.
 */
public class PurchaseServiceHandler extends AbstractHttpServiceHandler {

  @UseDataSet("history")
  private PurchaseHistoryStore store;

  /**
   * Responds id of the specified product.
   *
   * @param product A product name to retrieve an id for it, as example - "comb"
   */
  @Path("catalogLookup/{product}")
  @GET
  public void catalogLookup(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("product") String product) throws Exception {
    String productId = null;

    // Discover the CatalogLookup service via discovery service
    RuntimeContext runtimeContext = (RuntimeContext) this.getContext();
    URL serviceURL = runtimeContext.getServiceURL(PurchaseApp.APP_NAME, CatalogLookupService.SERVICE_NAME);
    if (serviceURL == null) {
      responder.sendError(HttpResponseStatus.NOT_FOUND.code(), "Catalog lookup service URL is null");
      return;
    }

    URL url = new URL(serviceURL, String.format("v1/product/%s/catalog", product));
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    if (HttpURLConnection.HTTP_OK == conn.getResponseCode()) {
      try {
        productId = new String(ByteStreams.toByteArray(conn.getInputStream()), Charsets.UTF_8);
      } finally {
        conn.disconnect();
      }
    }
    if (null == productId) {
      responder.sendError(HttpResponseStatus.NOT_FOUND.code(), String.format("No product id found for %s", product));
    } else {
      responder.sendJson(productId);
    }
  }

  /**
   * Responds the specified customer's purchases history in a JSON format.
   *
   * @param customer A customer name to retrieve a history for him, as example - "Tom"
   */
  @Path("history/{customer}")
  @GET
  public void history(HttpServiceRequest request, HttpServiceResponder responder,
                      @PathParam("customer") String customer) {
    PurchaseHistory history = store.read(customer);
    if (null == history) {
      responder.sendError(HttpResponseStatus.NOT_FOUND.code(),
                          String.format("No purchase history found for %s", customer));
    } else {
      responder.sendJson(history);
    }
  }
}
