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

package co.cask.cdap.test.app;

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.procedure.ProcedureResponse;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.AbstractServiceWorker;
import co.cask.cdap.api.service.ServiceWorkerContext;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * AppWithServices with a CentralService, which other programs will hit via their context's getServiceURL method.
 * This CentralService returns a constant string value {@link #ANSWER}, which is checked for in the test cases.
 */
public class AppUsingGetServiceURL extends AbstractApplication {
  public static final String APP_NAME = "AppUsingGetServiceURL";
  public static final String CENTRAL_SERVICE = "CentralService";
  public static final String SERVICE_WITH_WORKER = "ServiceWithWorker";
  public static final String PROCEDURE = "ForwardingProcedure";
  public static final String ANSWER = "MagicalString";
  public static final String DATASET_NAME = "SharedDataSet";
  public static final String DATASET_WHICH_KEY = "WhichKey";
  public static final String DATASET_KEY = "Key";
  public static final String WORKER_INSTANCES_DATASET = "WorkerInstancesDataset";

  @Override
  public void configure() {
    setName(APP_NAME);
    addProcedure(new ForwardingProcedure());
    addService(new CentralService());
    addService(new ServiceWithWorker());
    createDataset(DATASET_NAME, KeyValueTable.class);
    createDataset(WORKER_INSTANCES_DATASET, KeyValueTable.class);
  }


  public static final class ForwardingProcedure extends AbstractProcedure {

    @UseDataSet(DATASET_NAME)
    private KeyValueTable table;

    @Handle("ping")
    public void ping(ProcedureRequest request, ProcedureResponder responder) throws IOException {
      // Discover the CatalogLookup service via discovery service
      URL serviceURL = getContext().getServiceURL(CENTRAL_SERVICE);
      if (serviceURL == null) {
        responder.error(ProcedureResponse.Code.NOT_FOUND, "serviceURL is null");
        return;
      }
      URL url = new URL(serviceURL, "ping");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      try {
        if (HttpURLConnection.HTTP_OK == conn.getResponseCode()) {
          String response = new String(ByteStreams.toByteArray(conn.getInputStream()), Charsets.UTF_8);
          responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), response);
        } else {
          responder.error(ProcedureResponse.Code.FAILURE, "Failed to retrieve a response from the service");
        }
      } finally {
          conn.disconnect();
      }

    }

    @Handle("readDataSet")
    public void readDataSet(ProcedureRequest request, ProcedureResponder responder) throws IOException {
      String key = request.getArgument(DATASET_WHICH_KEY);
      byte[] value = table.read(key);
      if (value == null) {
        responder.error(ProcedureResponse.Code.NOT_FOUND, "Table returned null for value: " + key);
        return;
      }
      responder.sendJson(ProcedureResponse.Code.SUCCESS, Bytes.toString(value));
    }
  }

  /**
   * A service whose sole purpose is to check the ability for its worker to hit another service (via getServiceURL).
   */
  private static final class ServiceWithWorker extends AbstractService {

    @Override
    protected void configure() {
      setName(SERVICE_WITH_WORKER);
      addHandler(new NoOpHandler());
      addWorkers(new PingingWorker());
    }
    public static final class NoOpHandler extends AbstractHttpServiceHandler {
      // handles nothing.
    }

    private static final class PingingWorker extends AbstractServiceWorker {
      private static final Logger LOG = LoggerFactory.getLogger(PingingWorker.class);

      @Override
      protected void configure() {
        useDatasets(DATASET_NAME);
        useDatasets(WORKER_INSTANCES_DATASET);
        setInstances(5);
      }

      @Override
      public void initialize(ServiceWorkerContext context) throws Exception {
        super.initialize(context);

        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext context) throws Exception {
            KeyValueTable table = context.getDataset(WORKER_INSTANCES_DATASET);
            String key = String.format("%d.%d", getContext().getInstanceId(), System.nanoTime());
            table.write(key, Bytes.toBytes(getContext().getInstanceCount()));
          }
        });
      }

      private void writeToDataSet(final String key, final String val) {
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext context) throws Exception {
            KeyValueTable table = context.getDataset(DATASET_NAME);
            table.write(key, val);
          }
        });
      }

      @Override
      public void run() {
        URL baseURL = getContext().getServiceURL(CENTRAL_SERVICE);
        if (baseURL == null) {
          return;
        }

        URL url;
        String response;
        try {
          url = new URL(baseURL, "ping");
        } catch (MalformedURLException e) {
          return;
        }

        try {
          HttpURLConnection conn = (HttpURLConnection) url.openConnection();
          try {
            if (HttpURLConnection.HTTP_OK == conn.getResponseCode()) {
              response = new String(ByteStreams.toByteArray(conn.getInputStream()), Charsets.UTF_8);
              // Write the response to dataset, so that we can verify it from a test.
              writeToDataSet(DATASET_KEY, response);
            }
          } finally {
            conn.disconnect();
          }
        } catch (IOException e) {
          LOG.error("Got exception {}", e);
        }
      }
    }
  }

  /**
   * The central service which other programs will ping via their context's getServiceURL method.
   */
  private static final class CentralService extends AbstractService {

    @Override
    protected void configure() {
      setName("CentralService");
      addHandler(new NoOpHandler());
    }

    public static final class NoOpHandler extends AbstractHttpServiceHandler {
      @Path("/ping")
      @GET
      public void handler(HttpServiceRequest request, HttpServiceResponder responder) {
        responder.sendString(200, ANSWER, Charsets.UTF_8);
      }
    }
  }
}
