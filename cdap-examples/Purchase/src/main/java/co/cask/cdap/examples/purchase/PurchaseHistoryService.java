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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.Service;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.sql.Driver;
import java.util.HashMap;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * A {@link Service} for querying a customer's purchase history from a Dataset.
 */
public class PurchaseHistoryService extends AbstractService {
  private static final Logger LOG = LoggerFactory.getLogger(PurchaseHistoryService.class);
  public static final String SERVICE_NAME = "PurchaseHistoryService";

  @Override
  protected void configure() {
    setName(SERVICE_NAME);
    setDescription("A service to retrieve a customer's purchase history");
    addHandler(new PurchaseHistoryServiceHandler());
    setResources(new Resources(1024));
  }

  /**
   * Service for retrieving a customer’s purchase history.
   */
  public static final class PurchaseHistoryServiceHandler extends AbstractHttpServiceHandler {

    @UseDataSet("history")
    private PurchaseHistoryStore store;

    @Override
    public void initialize(HttpServiceContext context) throws Exception {
      try {
        // Load plugin in service
        super.initialize(context);
      } catch (Exception e) {
        LOG.error("Failed to load the JDBCPlugin", e);
      }
    }

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

    /**
     * Retrieves a specified customer's purchase history in a JSON format.
     *
     * @param plugin name to load
     */
    @Path("load/{plugin}")
    @GET
    public void load(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("plugin") String plugin) {
      try {
        HttpServiceContext context = getContext();

        if (context == null) {
          responder.sendError(500, "Context is null");
          return;
        }

        Driver driver = getContext().newPluginInstance("jdbc", plugin,
                                                                        PluginProperties.builder().build());
        LOG.info("Driver class Major version {} Minor Version {} JDBC Compliant {}", driver.getMajorVersion(),
                 driver.getMinorVersion(), driver.jdbcCompliant());
        responder.sendJson(200, String.format("Loaded plugin successfully %s", plugin));
      } catch (Exception e) {
        LOG.error("Exception while loading plugin", e);
        responder.sendError(500, String.format("Error While loading plugin %s, %s", plugin, e));
      }
    }
  }
}
