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
import co.cask.cdap.api.service.AbstractServiceWorker;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.internal.app.services.GuavaServiceWorker;
import com.google.common.base.Charsets;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * A Catalog Lookup Service implementation that provides ids for products.
 */
public class CatalogLookupService extends AbstractService {

  /**
   * Example Guava Service which simply writes to LOG once every 3 seconds.
   */
  public static final class NoOpGuavaWorker extends AbstractScheduledService {
    private static final Logger LOG = LoggerFactory.getLogger(NoOpGuavaWorker.class);
    private int numIterations;

    @Override
    protected void runOneIteration() throws Exception {
      numIterations++;
      LOG.info("{} completed iteration #{}", this.getClass().getSimpleName(), numIterations);
    }

    @Override
    protected Scheduler scheduler() {
      return Scheduler.newFixedDelaySchedule(0, 3, TimeUnit.SECONDS);
    }
  }

  /**
   * Example ServiceWorker which simply writes to LOG once every 3 seconds.
   */
  public static final class NoOpServiceWorker extends AbstractServiceWorker {
    private static final Logger LOG = LoggerFactory.getLogger(NoOpServiceWorker.class);
    private int numIterations;

    @Override
    public void stop() {
      //no-op
    }

    @Override
    public void destroy() {
      //no-op
    }

    @Override
    public void run() {
      while (true) {
        numIterations++;
        LOG.info("{} completed iteration #{}", this.getClass().getSimpleName(), numIterations);
        try {
          Thread.sleep(3000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  @Override
  protected void configure() {
    setName(PurchaseApp.SERVICE_NAME);
    setDescription("Service to lookup product ids.");
    addHandler(new ProductCatalogLookup());
    addWorker(new NoOpServiceWorker());
    addWorker(new GuavaServiceWorker(new NoOpGuavaWorker()));
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
