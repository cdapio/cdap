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

import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * A Catalog Lookup Service implementation that provides ids for products.
 */
public class CatalogLookupService extends AbstractService {

  public static final String SERVICE_NAME = "CatalogLookup";

  @Override
  protected void configure() {
    setName(SERVICE_NAME);
    setDescription("Service to lookup product ids.");
    addHandler(new ProductCatalogLookup());
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
