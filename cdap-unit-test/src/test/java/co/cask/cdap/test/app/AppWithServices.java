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
import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.procedure.ProcedureResponse;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.AbstractServiceWorker;
import co.cask.cdap.api.service.BasicService;
import co.cask.cdap.api.service.ServiceWorkerContext;
import co.cask.cdap.api.service.TxRunnable;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * AppWithServices with a DummyService for unit testing.
 */
public class AppWithServices extends AbstractApplication {
  public static final String APP_NAME = "AppWithServices";
  public static final String SERVICE_NAME = "ServerService";
  public static final String DATASET_WORKER_SERVICE_NAME = "DatasetUpdateService";
  public static final String DATASET_TEST_KEY = "testKey";
  public static final String DATASET_TEST_VALUE = "testValue";
  public static final String DATASET_TEST_KEY_STOP = "testKeyStop";
  public static final String DATASET_TEST_VALUE_STOP = "testValueStop";
  public static final String PROCEDURE_DATASET_KEY = "key";

  private static final String DATASET_NAME = "AppWithServicesDataset";
  private static final String INIT_KEY = "init";

  public static final String TRANSACTIONS_SERVICE_NAME = "TransactionsTestService";
  public static final String TRANSACTIONS_DATASET_NAME = "TransactionsDatasetName";
  public static final String DESTROY_KEY = "destroy";
  public static final String VALUE = "true";

  public static final String WRITE_VALUE_RUN_KEY = "write.value.run";
  public static final String WRITE_VALUE_STOP_KEY = "write.value.stop";

    @Override
    public void configure() {
      setName(APP_NAME);
      addStream(new Stream("text"));
      addProcedure(new NoOpProcedure());
      addService(new BasicService(SERVICE_NAME, new ServerService()));
      addService(new DatasetUpdateService());
      addService(new TransactionalHandlerService());
      createDataset(DATASET_NAME, KeyValueTable.class);
      createDataset(TRANSACTIONS_DATASET_NAME, KeyValueTable.class);
   }


  public static final class NoOpProcedure extends AbstractProcedure {

    @UseDataSet(DATASET_NAME)
    private KeyValueTable table;

    @Handle("ping")
    public void ping(ProcedureRequest request, ProcedureResponder responder) throws IOException {
      String key = request.getArgument(PROCEDURE_DATASET_KEY);
      responder.sendJson(ProcedureResponse.Code.SUCCESS, Bytes.toString(table.read(key)));
    }
  }

  public static class TransactionalHandlerService extends AbstractService {

    @Override
    protected void configure() {
      setName(TRANSACTIONS_SERVICE_NAME);
      addHandler(new TransactionsHandler());
    }

    public static final class TransactionsHandler extends AbstractHttpServiceHandler {

      @UseDataSet(TRANSACTIONS_DATASET_NAME)
      KeyValueTable table;

      @Override
      public void initialize(HttpServiceContext context) throws Exception {
        super.initialize(context);
        table.write(INIT_KEY, VALUE);
      }

      @Path("/write/{key}/{value}/{sleep}")
      @GET
      public void handler(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("key") String key, @PathParam("value") String value, @PathParam("sleep") int sleep)
        throws InterruptedException {
        //Check if data written in initialize method is persisted.
        Preconditions.checkArgument(Bytes.toString(table.read(INIT_KEY)).equals(VALUE));
        table.write(key, value);
        Thread.sleep(sleep);
        responder.sendStatus(200);
      }

      @Path("/read/{key}")
      @GET
      public void readHandler(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("key") String key) {
        String value = Bytes.toString(table.read(key));
        if (value == null) {
          responder.sendStatus(204);
        } else {
          responder.sendJson(200, value);
        }
      }

      @Override
      public void destroy() {
        super.destroy();
        table.write(DESTROY_KEY, VALUE);
      }
    }
  }

  @Path("/")
  public class ServerService extends AbstractHttpServiceHandler {

    @Path("/ping2")
    @GET
    public void handler(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendStatus(200);
    }

    @Path("/failure")
    @GET
    public void failure(HttpServiceRequest request, HttpServiceResponder responder) {
      throw new IllegalStateException("Failed");
    }

    @Path("verifyClassLoader")
    @GET
    public void verifyClassLoader(HttpServiceRequest request, HttpServiceResponder responder) {
      if (Thread.currentThread().getContextClassLoader() != getClass().getClassLoader()) {
        responder.sendStatus(500);
      } else {
        responder.sendStatus(200);
      }
    }

    @Path("/discover/{app}/{service}")
    @GET
    public void discoverService(HttpServiceRequest request, HttpServiceResponder responder,
                                @PathParam("app") String appId, @PathParam("service") String serviceId) {
      URL url = getContext().getServiceURL(appId, serviceId);
      if (url == null) {
        responder.sendStatus(HttpURLConnection.HTTP_NO_CONTENT);
      } else {
        responder.sendJson(url);
      }
    }
  }

  private static final class DatasetUpdateService extends AbstractService {

    @Override
    protected void configure() {
      setName(DATASET_WORKER_SERVICE_NAME);
      addHandler(new NoOpHandler());
      addWorker("updater", new DatasetUpdateWorker(DATASET_NAME));
    }

    private static final class NoOpHandler extends AbstractHttpServiceHandler {
      // no-op
    }

    private static final class DatasetUpdateWorker extends AbstractServiceWorker {

      private static int datasetHashCode;
      private volatile boolean workerStopped = false;

      @Property
      private long sleepMs = 1000;

      private String dataset;
      private String valueToWriteOnRun;
      private String valueToWriteOnStop;

      public DatasetUpdateWorker(String dataset) {
        // Remember the dataset name so that it can set in configure time.
        // It's done like this to test the @Property is working.
        this.dataset = dataset;
      }

      @Override
      protected void configure() {
        useDatasets(dataset);
      }

      @Override
      public void initialize(ServiceWorkerContext context) throws Exception {
        super.initialize(context);
        valueToWriteOnRun = context.getRuntimeArguments().get(WRITE_VALUE_RUN_KEY);
        valueToWriteOnStop = context.getRuntimeArguments().get(WRITE_VALUE_STOP_KEY);
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext context) throws Exception {
            KeyValueTable table = context.getDataset(DATASET_NAME);
            datasetHashCode = System.identityHashCode(table);
          }
        });
      }

      @Override
      public void stop() {
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext context) throws Exception {
            KeyValueTable table = context.getDataset(DATASET_NAME);
            table.write(DATASET_TEST_KEY_STOP, valueToWriteOnStop);
          }
        });
        workerStopped = true;
      }

      @Override
      public void run() {
        try {
          // Run this loop till stop is called.
          while (!workerStopped) {
            getContext().execute(new TxRunnable() {
              @Override
              public void run(DatasetContext context) throws Exception {
                KeyValueTable table = context.getDataset(DATASET_NAME);
                // Write only if the dataset instance is the same as the one gotten in initialize.
                if (datasetHashCode == System.identityHashCode(table)) {
                  table.write(DATASET_TEST_KEY, valueToWriteOnRun);
                }
              }
            });
            TimeUnit.MILLISECONDS.sleep(sleepMs);
          }
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    }
  }
}
