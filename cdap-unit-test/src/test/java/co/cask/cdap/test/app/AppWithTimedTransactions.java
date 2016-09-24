/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpContentConsumer;
import co.cask.cdap.api.service.http.HttpContentProducer;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.worker.AbstractWorker;
import co.cask.cdap.api.worker.WorkerContext;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.test.RevealingTxSystemClient;
import com.google.common.base.Throwables;
import org.apache.http.entity.ContentType;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionFailureException;
import org.junit.Assert;

import java.nio.ByteBuffer;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

/**
 * An app that starts transactions with custom timeout and validates the timeout using a custom dataset.
 * This relies on TestBase to inject {@link RevealingTxSystemClient} for this test.
 */
public class AppWithTimedTransactions extends AbstractApplication {

  public static final String NAME = "AppWithTimedTransactions";
  public static final String SERVICE = "TimeoutService";
  public static final String WORKER = "TimeoutWorker";
  public static final String WORKFLOW = "TimeoutWorkflow";
  public static final String ACTION = "TimeoutAction";
  public static final String CONSUMER = "HttpContentConsumer";
  public static final String PRODUCER = "HttpContentProducer";
  public static final String CAPTURE = "capture";
  public static final String INITIALIZE = "initialize";
  public static final String DESTROY = "destroy";
  public static final String RUNTIME = "runtime";

  public static final int TIMEOUT_WORKER_INITIALIZE = 20;
  public static final int TIMEOUT_WORKER_DESTROY = 21;
  public static final int TIMEOUT_WORKER_RUNTIME = 22;
  public static final int TIMEOUT_CONSUMER_RUNTIME = 23;
  public static final int TIMEOUT_PRODUCER_RUNTIME = 24;
  public static final int TIMEOUT_ACTION_RUNTIME = 25;

  @Override
  public void configure() {
    setName(NAME);
    createDataset(CAPTURE, CapturingDataset.class);
    addWorker(new TimeoutWorker());
    addService(new AbstractService() {
      @Override
      protected void configure() {
        setName(SERVICE);
        addHandler(new TimeoutHandler());
      }
    });
    addWorkflow(new AbstractWorkflow() {
      @Override
      protected void configure() {
        setName(WORKFLOW);
        addAction(new TimeoutAction());
      }
    });
  }

  /**
   * Uses the provided Transactional with the given timeout, and validates that the transaction that is started
   * was actually given that timeout.
   */
  static void recordTimedTransaction(Transactional transactional, final String where, final String key, int timeout) {
    try {
      transactional.execute(timeout, new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          CapturingDataset capture = context.getDataset(CAPTURE);
          Transaction tx = capture.getTx();
          // we cannot cast because the RevealingTransaction is not visible in the program class loader
          Assert.assertEquals("RevealingTransaction", tx.getClass().getSimpleName());
          int txTimeout = (int) tx.getClass().getField("timeout").get(tx);
          Table t = capture.getTable();
          t.put(new Put(where, key, String.valueOf(txTimeout)));
        }
      });
    } catch (TransactionFailureException e) {
      throw Throwables.propagate(e);
    }
  }

  private static class TimeoutWorker extends AbstractWorker {

    @Override
    protected void configure() {
      setName(WORKER);
    }

    @Override
    public void initialize(WorkerContext context) throws Exception {
      super.initialize(context);
      recordTimedTransaction(context, WORKER, INITIALIZE, TIMEOUT_WORKER_INITIALIZE);
    }

    @Override
    public void run() {
      recordTimedTransaction(getContext(), WORKER, RUNTIME, TIMEOUT_WORKER_RUNTIME);
    }

    @Override
    public void destroy() {
      recordTimedTransaction(getContext(), WORKER, DESTROY, TIMEOUT_WORKER_DESTROY);
      super.destroy();
    }
  }

  public static class TimeoutHandler extends AbstractHttpServiceHandler {

    // service context does not have Transactional, no need to test lifecycle methods

    @PUT
    @Path("test")
    public HttpContentConsumer handle(HttpServiceRequest request, HttpServiceResponder responder) {
      return new HttpContentConsumer() {

        @Override
        public void onReceived(ByteBuffer chunk, Transactional transactional) throws Exception {
          recordTimedTransaction(transactional, CONSUMER, RUNTIME, TIMEOUT_CONSUMER_RUNTIME);
        }

        @Override
        public void onFinish(HttpServiceResponder responder) throws Exception {
          responder.send(200, new HttpContentProducer() {

            @Override
            public ByteBuffer nextChunk(Transactional transactional) throws Exception {
              recordTimedTransaction(transactional, PRODUCER, RUNTIME, TIMEOUT_PRODUCER_RUNTIME);
              return ByteBuffer.allocate(0);
            }

            @Override
            public void onFinish() throws Exception {
            }

            @Override
            public void onError(Throwable failureCause) {
            }
          }, ContentType.TEXT_PLAIN.getMimeType());
        }

        @Override
        public void onError(HttpServiceResponder responder, Throwable failureCause) {
        }
      };
    }
  }

  public static class TimeoutAction extends AbstractCustomAction {

    @Override
    protected void configure() {
      setName(ACTION);
    }

    @Override
    public void run() throws Exception {
      recordTimedTransaction(getContext(), ACTION, RUNTIME,  TIMEOUT_ACTION_RUNTIME);
    }
  }
}
