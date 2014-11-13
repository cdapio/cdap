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
import co.cask.cdap.api.data.DataSetContext;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.procedure.ProcedureResponse;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.AbstractServiceWorker;
import co.cask.cdap.api.service.TxRunnable;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

import java.io.IOException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * AppWithServices with a DummyService for unit testing.
 */
public class AppWithServices extends AbstractApplication {
  public static final String SERVICE_NAME = "ServerService";
  public static final String DATASET_WORKER_SERVICE_NAME = "DatasetUpdateService";
  public static final String DATASET_TEST_KEY = "testKey";
  public static final String DATASET_TEST_VALUE = "testValue";
  public static final String DATASET_TEST_KEY_STOP = "testKeyStop";
  public static final String DATASET_TEST_VALUE_STOP = "testValueStop";
  public static final String PROCEDURE_DATASET_KEY = "key";

  private static final String DATASET_NAME = "AppWithServicesDataset";

  public static final String TRANSACTIONS_SERVICE_NAME = "TransactionsTestService";
  private static final String TRANSACTIONS_DATASET_NAME = "TransactionsDatasetName";

    @Override
    public void configure() {
      setName("AppWithServices");
      addStream(new Stream("text"));
      addProcedure(new NoOpProcedure());
      addService(SERVICE_NAME, new ServerService());
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

      @Path("/write/{key}/{value}/{sleep}")
      @GET
      public void handler(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("key") String key, @PathParam("value") String value, @PathParam("sleep") int sleep)
        throws InterruptedException {
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
  }

  private static final class DatasetUpdateService extends AbstractService {

    @Override
    protected void configure() {
      setName(DATASET_WORKER_SERVICE_NAME);
      addHandler(new NoOpHandler());
      addWorker(new DatasetUpdateWorker());
      useDataset(DATASET_NAME);
    }

    private static final class NoOpHandler extends AbstractHttpServiceHandler {
      // no-op
    }

    private static final class DatasetUpdateWorker extends AbstractServiceWorker {
      private volatile boolean workerStopped = false;

      @Override
      public void stop() {
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DataSetContext context) throws Exception {
            KeyValueTable table = context.getDataSet(DATASET_NAME);
            table.write(DATASET_TEST_KEY_STOP, DATASET_TEST_VALUE_STOP);
          }
        });
        workerStopped = true;
      }

      @Override
      public void run() {
        // Run this loop till stop is called.
        while (!workerStopped) {
          getContext().execute(new TxRunnable() {
            @Override
            public void run(DataSetContext context) throws Exception {
              KeyValueTable table = context.getDataSet(DATASET_NAME);
              table.write(DATASET_TEST_KEY, DATASET_TEST_VALUE);
            }
          });
        }
      }
    }
  }
}
