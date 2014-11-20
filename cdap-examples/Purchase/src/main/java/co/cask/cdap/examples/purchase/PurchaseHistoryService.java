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

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.Service;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;

import java.net.HttpURLConnection;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * A {@link Service} for querying a customer's purchase history from a Dataset.
 */
public class PurchaseHistoryService extends AbstractService {

  public static final String SERVICE_NAME = "PurchaseHistoryService";

  @Override
  protected void configure() {
    setName(SERVICE_NAME);
    setDescription("A service to retrieve a customer's purchase history");
    addHandler(new PurchaseHistoryServiceHandler());
  }

  /**
   * Service for retrieving a customer’s purchase history.
   */
  public static final class PurchaseHistoryServiceHandler extends AbstractHttpServiceHandler {

    @UseDataSet("history")
    private PurchaseHistoryStore store;

    /**
     * Retrieves a specified customer's purchase history in a JSON format.
     *
     * @param customer name of customer whose history is to be retrieved
     */
    @Path("history/{customer}")
    @GET
    public void history(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("customer") String customer) {
      PurchaseHistory history = store.read(customer);
      if (history == null) {
        responder.sendString(HttpURLConnection.HTTP_NO_CONTENT,
                             String.format("No purchase history found for %s", customer), Charsets.UTF_8);
      } else {
        responder.sendJson(history);
      }
    }
  }
}
