/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.BasicService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.worker.AbstractWorker;
import co.cask.cdap.api.worker.WorkerContext;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * AppWithServices with a CentralService, which other programs will hit via their context's getServiceURL method.
 * This CentralService returns a constant string value {@link #ANSWER}, which is checked for in the test cases.
 */
public class AppUsingGetServiceURL extends AbstractApplication {
  public static final String APP_NAME = "AppUsingGetServiceURL";
  public static final String CENTRAL_SERVICE = "CentralService";
  public static final String LIFECYCLE_WORKER = "LifecycleWorker";
  public static final String PINGING_WORKER = "PingingWorker";
  public static final String FORWARDING = "ForwardingService";
  public static final String ANSWER = "MagicalString";
  public static final String DATASET_NAME = "SharedDataSet";
  public static final String DATASET_WHICH_KEY = "WhichKey";
  public static final String DATASET_KEY = "Key";
  public static final String WORKER_INSTANCES_DATASET = "WorkerInstancesDataset";

  @Override
  public void configure() {
    setName(APP_NAME);
    addService(new BasicService("ForwardingService", new ForwardingHandler()));
    addService(new CentralService());
    addWorker(new PingingWorker());
    addWorker(new LifecycleWorker());
    createDataset(DATASET_NAME, KeyValueTable.class);
    createDataset(WORKER_INSTANCES_DATASET, KeyValueTable.class);
  }

  /**
   *
   */
  public static final class ForwardingHandler extends AbstractHttpServiceHandler {

    @UseDataSet(DATASET_NAME)
    private KeyValueTable table;

    @GET
    @Path("ping")
    public void ping(HttpServiceRequest request, HttpServiceResponder responder) throws IOException {
      // Discover the CatalogLookup service via discovery service
      URL serviceURL = getContext().getServiceURL(CENTRAL_SERVICE);
      if (serviceURL == null) {
        responder.sendError(404, "serviceURL is null");
        return;
      }
      URL url = new URL(serviceURL, "ping");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      try {
        if (HttpURLConnection.HTTP_OK == conn.getResponseCode()) {
          String response = new String(ByteStreams.toByteArray(conn.getInputStream()), Charsets.UTF_8);
          responder.sendJson(response);
        } else {
          responder.sendError(500, "Failed to retrieve a response from the service");
        }
      } finally {
          conn.disconnect();
      }
    }

    @GET
    @Path("read/{key}")
    public void readDataSet(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("key") String key) throws IOException {
      byte[] value = table.read(key);
      if (value == null) {
        responder.sendError(404, "Table returned null for value: " + key);
        return;
      }
      responder.sendJson(Bytes.toString(value));
    }
  }

  private static void writeToDataSet(final WorkerContext context,
                                     final String tableName, final String key, final byte[] value) {
    context.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        KeyValueTable table = context.getDataset(tableName);
        table.write(key, value);
      }
    });
  }

  /**
   *
   */
  public static final class LifecycleWorker extends AbstractWorker {
    private static final Logger LOG = LoggerFactory.getLogger(LifecycleWorker.class);
    private volatile boolean isRunning;

    @Override
    protected void configure() {
      setName(LIFECYCLE_WORKER);
      useDatasets(WORKER_INSTANCES_DATASET);
      setInstances(3);
    }

    @Override
    public void initialize(WorkerContext context) throws Exception {
      super.initialize(context);

      String key = String.format("init.%d", getContext().getInstanceId());
      byte[] value = Bytes.toBytes(getContext().getInstanceCount());
      writeToDataSet(getContext(), WORKER_INSTANCES_DATASET, key, value);
    }

    @Override
    public void run() {
      isRunning = true;
      while (isRunning) {
        try {
          TimeUnit.MILLISECONDS.sleep(50);
        } catch (InterruptedException e) {
          LOG.error("Error sleeping in LifecycleWorker", e);
        }
      }
    }

    @Override
    public void stop() {
      isRunning = false;

      String key = String.format("stop.%d", getContext().getInstanceId());
      byte[] value = Bytes.toBytes(getContext().getInstanceCount());
      writeToDataSet(getContext(), WORKER_INSTANCES_DATASET, key, value);
    }
  }

  /**
   *
   */
  public static final class PingingWorker extends AbstractWorker {
    private static final Logger LOG = LoggerFactory.getLogger(PingingWorker.class);

    @Override
    protected void configure() {
      setName(PINGING_WORKER);
      useDatasets(DATASET_NAME);
      setInstances(5);
    }

    @Override
    public void run() {
      URL baseURL = getContext().getServiceURL(CENTRAL_SERVICE);
      if (baseURL == null) {
        return;
      }

      URL url;
      try {
        url = new URL(baseURL, "ping");
      } catch (MalformedURLException e) {
        return;
      }

      try {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        try {
          if (HttpURLConnection.HTTP_OK == conn.getResponseCode()) {
            // Write the response to dataset, so that we can verify it from a test.
            writeToDataSet(getContext(), DATASET_NAME, DATASET_KEY, ByteStreams.toByteArray(conn.getInputStream()));
          }
        } finally {
          conn.disconnect();
        }
      } catch (IOException e) {
        LOG.error("Got exception {}", e);
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
      addHandler(new PingHandler());
    }

    public static final class PingHandler extends AbstractHttpServiceHandler {
      @Path("/ping")
      @GET
      public void handler(HttpServiceRequest request, HttpServiceResponder responder) {
        responder.sendString(200, ANSWER, Charsets.UTF_8);
      }
    }
  }
}
