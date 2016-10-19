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
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.customaction.AbstractCustomAction;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpContentConsumer;
import co.cask.cdap.api.service.http.HttpContentProducer;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.worker.AbstractWorker;
import co.cask.cdap.api.worker.WorkerContext;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.test.RevealingTxSystemClient;
import co.cask.cdap.test.RevealingTxSystemClient.RevealingTransaction;
import com.google.common.base.Throwables;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.http.entity.ContentType;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

/**
 * An app that starts transactions with custom timeout and validates the timeout using a custom dataset.
 * This app also has methods with @TransactionPolicy annotations, to validate that these don't get run inside a tx.
 * These methods will then start transactions explicitly, and attempt to nest transactions.
 *
 * This relies on TestBase to inject {@link RevealingTxSystemClient} for this test.
 */
@SuppressWarnings("WeakerAccess")
public class AppWithCustomTx extends AbstractApplication {

  private static final Logger LOG = LoggerFactory.getLogger(AppWithCustomTx.class);

  private static final String NAME = "AppWithCustomTx";
  static final String CAPTURE = "capture";
  static final String INPUT = "input";
  static final String DEFAULT = "default";
  static final String FAILED = "failed";

  static final String ACTION_TX = "TxAction";
  static final String ACTION_NOTX = "NoTxAction";
  static final String CONSUMER = "HttpContentConsumer";
  static final String FLOW = "TimedTxFlow";
  static final String FLOWLET_TX = "TxFlowlet";
  static final String FLOWLET_NOTX = "NoTxFlowlet";
  static final String MAPREDUCE_NOTX = "NoTxMR";
  static final String MAPREDUCE_TX = "TxMR";
  static final String PRODUCER = "HttpContentProducer";
  static final String SERVICE = "TimedTxService";
  static final String SPARK_NOTX = "NoTxSpark";
  static final String SPARK_TX = "TxSpark";
  static final String WORKER_TX = "TxWorker";
  static final String WORKER_NOTX = "NoTxWorker";
  static final String WORKFLOW_TX = "TxWorkflow";
  static final String WORKFLOW_NOTX = "NoTxWorkflow";

  static final String INITIALIZE = "initialize";
  static final String INITIALIZE_TX = "initialize-tx";
  static final String INITIALIZE_NEST = "initialize-nest";
  static final String DESTROY = "destroy";
  static final String DESTROY_TX = "destroy-tx";
  static final String DESTROY_NEST = "destroy-nest";
  static final String RUNTIME = "runtime";
  static final String RUNTIME_TX = "runtime-tx";
  static final String RUNTIME_NEST = "runtime-nest";

  static final int TIMEOUT_ACTION_RUNTIME = 13;
  static final int TIMEOUT_ACTION_DESTROY = 14;
  static final int TIMEOUT_ACTION_INITIALIZE = 15;
  static final int TIMEOUT_CONSUMER_RUNTIME = 16;
  static final int TIMEOUT_FLOWLET_DESTROY = 17;
  static final int TIMEOUT_FLOWLET_INITIALIZE = 18;
  static final int TIMEOUT_MAPREDUCE_DESTROY = 19;
  static final int TIMEOUT_MAPREDUCE_INITIALIZE = 20;
  static final int TIMEOUT_PRODUCER_RUNTIME = 21;
  static final int TIMEOUT_SPARK_DESTROY = 22;
  static final int TIMEOUT_SPARK_INITIALIZE = 23;
  static final int TIMEOUT_WORKER_DESTROY = 24;
  static final int TIMEOUT_WORKER_INITIALIZE = 25;
  static final int TIMEOUT_WORKER_RUNTIME = 26;
  static final int TIMEOUT_WORKFLOW_DESTROY = 27;
  static final int TIMEOUT_WORKFLOW_INITIALIZE = 28;

  @Override
  public void configure() {
    setName(NAME);
    addStream(INPUT);
    createDataset(CAPTURE, TransactionCapturingTable.class);
    addWorker(new NoTxWorker());
    addWorker(new TxWorker());
    addMapReduce(new TxMR());
    addMapReduce(new NoTxMR());
    addSpark(new SparkWithCustomTx.TxSpark());
    addSpark(new SparkWithCustomTx.NoTxSpark());
    addWorkflow(new TxWorkflow());
    addWorkflow(new NoTxWorkflow());
    addService(new AbstractService() {
      @Override
      protected void configure() {
        setName(SERVICE);
        addHandler(new TimeoutHandler());
      }
    });
    addFlow(new AbstractFlow() {
      @Override
      protected void configure() {
        setName(FLOW);
        addFlowlet(FLOWLET_TX, new TxFlowlet());
        addFlowlet(FLOWLET_NOTX, new NoTxFlowlet());
        connectStream(INPUT, FLOWLET_TX);
        connect(FLOWLET_TX, FLOWLET_NOTX);
      }
    });
  }

  /**
   * Uses the provided Transactional with the given timeout, and records the timeout that the transaction
   * was actually given, or "default" if no explicit timeout was given.
   */
  static void executeRecordTransaction(Transactional transactional,
                                       final String row, final String column, int timeout) {
    try {
      transactional.execute(timeout, new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          recordTransaction(context, row, column);
        }
      });
    } catch (TransactionFailureException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * If in a transaction, records the timeout that the current transaction was given, or "default" if no explicit
   * timeout was given. Otherwise does nothing.
   *
   * Note: we know whether and what explicit timeout was given, because we inject a {@link RevealingTxSystemClient},
   *       which returns a {@link RevealingTransaction} for {@link TransactionSystemClient#startShort(int)} only.
   */
  static void recordTransaction(DatasetContext context, String row, String column) {
    TransactionCapturingTable capture = context.getDataset(CAPTURE);
    Transaction tx = capture.getTx();
    // we cannot cast because the RevealingTransaction is not visible in the program class loader
    String value = DEFAULT;
    if (tx == null) {
      try {
        capture.getTable().put(new Put(row, column, value));
        throw new RuntimeException("put to table without transaction should have failed.");
      } catch (DataSetException e) {
        // expected
      }
      return;
    }
    if ("RevealingTransaction".equals(tx.getClass().getSimpleName())) {
      int txTimeout;
      try {
        txTimeout = (int) tx.getClass().getField("timeout").get(tx);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
      value = String.valueOf(txTimeout);
    }
    capture.getTable().put(new Put(row, column, value));
  }

  /**
   * Attempt to nest transactions. we expect this to fail, and if it does, we write the value "failed"
   * to the table, for the test case to validate.
   */
  static void attemptNestedTransaction(Transactional txnl, final String row, final String key) {
    try {
      txnl.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext ctext) throws Exception {
          recordTransaction(ctext, row, key);
        }
      });
      LOG.error("Nested transaction should not have succeeded for {}:{}", row, key);
    } catch (TransactionFailureException e) {
      // expected: starting nested transaction should fail
      LOG.info("Nested transaction failed as expected for {}:{}", row, key);
    } catch (RuntimeException e) {
      // TODO (CDAP-6837): this is needed because worker's execute() propagates the tx failure as a runtime exception
      if (e.getCause() instanceof TransactionFailureException) {
        // expected: starting nested transaction should fail
        LOG.info("Nested transaction failed as expected for {}:{}", row, key);
      } else {
        throw e;
      }
    }
    // we know that the transactional is a program context and hence implement DatasetContext
    TransactionCapturingTable capture = ((DatasetContext) txnl).getDataset(CAPTURE);
    capture.getTable().put(new Put(row, key, FAILED));
  }

  public static class NoTxWorker extends AbstractWorker {

    @Override
    protected void configure() {
      setName(WORKER_NOTX);
    }

    @Override
    public void initialize(WorkerContext context) throws Exception {
      super.initialize(context);
      recordTransaction(getContext(), WORKER_NOTX, INITIALIZE);
      executeRecordTransaction(context, WORKER_NOTX, INITIALIZE_TX, TIMEOUT_WORKER_INITIALIZE);
      context.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext ctext) throws Exception {
          attemptNestedTransaction(getContext(), WORKER_NOTX, INITIALIZE_NEST);
        }
      });
    }

    @Override
    public void run() {
      recordTransaction(getContext(), WORKER_NOTX, RUNTIME);
      executeRecordTransaction(getContext(), WORKER_NOTX, RUNTIME_TX, TIMEOUT_WORKER_RUNTIME);
      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext ctext) throws Exception {
          attemptNestedTransaction(getContext(), WORKER_NOTX, RUNTIME_NEST);
        }
      });
    }

    @Override
    public void destroy() {
      recordTransaction(getContext(), WORKER_NOTX, DESTROY);
      executeRecordTransaction(getContext(), WORKER_NOTX, DESTROY_TX, TIMEOUT_WORKER_DESTROY);
      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext ctext) throws Exception {
          attemptNestedTransaction(getContext(), WORKER_NOTX, DESTROY_NEST);
        }
      });
      super.destroy();
    }
  }

  public static class TxWorker extends AbstractWorker {

    @Override
    protected void configure() {
      setName(WORKER_TX);
    }

    @Override
    @TransactionPolicy(TransactionControl.IMPLICIT)
    public void initialize(WorkerContext context) throws Exception {
      super.initialize(context);
      recordTransaction(getContext(), WORKER_TX, INITIALIZE);
      attemptNestedTransaction(getContext(), WORKER_TX, INITIALIZE_NEST);
    }

    @Override
    public void run() {
      recordTransaction(getContext(), WORKER_TX, RUNTIME);
      executeRecordTransaction(getContext(), WORKER_TX, RUNTIME_TX, TIMEOUT_WORKER_RUNTIME);
      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext ctext) throws Exception {
          attemptNestedTransaction(getContext(), WORKER_TX, RUNTIME_NEST);
        }
      });
    }

    @Override
    @TransactionPolicy(TransactionControl.IMPLICIT)
    public void destroy() {
      recordTransaction(getContext(), WORKER_TX, DESTROY);
      attemptNestedTransaction(getContext(), WORKER_TX, DESTROY_NEST);
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
          executeRecordTransaction(transactional, CONSUMER, RUNTIME_TX, TIMEOUT_CONSUMER_RUNTIME);
        }

        @Override
        public void onFinish(HttpServiceResponder responder) throws Exception {
          responder.send(200, new HttpContentProducer() {

            @Override
            public ByteBuffer nextChunk(Transactional transactional) throws Exception {
              executeRecordTransaction(transactional, PRODUCER, RUNTIME_TX, TIMEOUT_PRODUCER_RUNTIME);
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

  private static class TxWorkflow extends AbstractWorkflow {
    @Override
    protected void configure() {
      setName(WORKFLOW_TX);
      addAction(new TxAction());
    }

    @Override
    public void initialize(WorkflowContext context) throws Exception {
      super.initialize(context);
      recordTransaction(context, WORKFLOW_TX, INITIALIZE);
      attemptNestedTransaction(context, WORKFLOW_TX, INITIALIZE_NEST);
    }

    @Override
    public void destroy() {
      super.destroy();
      recordTransaction(getContext(), WORKFLOW_TX, DESTROY);
      attemptNestedTransaction(getContext(), WORKFLOW_TX, DESTROY_NEST);
    }
  }

  private static class NoTxWorkflow extends AbstractWorkflow {
    @Override
    protected void configure() {
      setName(WORKFLOW_NOTX);
      addAction(new NoTxAction());
    }

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public void initialize(WorkflowContext context) throws Exception {
      super.initialize(context);
      recordTransaction(context, WORKFLOW_NOTX, INITIALIZE);
      executeRecordTransaction(getContext(), WORKFLOW_NOTX, INITIALIZE_TX, TIMEOUT_WORKFLOW_INITIALIZE);
      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext ctext) throws Exception {
          attemptNestedTransaction(getContext(), WORKFLOW_NOTX, INITIALIZE_NEST);
        }
      });
    }

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public void destroy() {
      super.destroy();
      recordTransaction(getContext(), WORKFLOW_NOTX, DESTROY);
      executeRecordTransaction(getContext(), WORKFLOW_NOTX, DESTROY_TX, TIMEOUT_WORKFLOW_DESTROY);
      try {
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext ctext) throws Exception {
            attemptNestedTransaction(getContext(), WORKFLOW_NOTX, DESTROY_NEST);
          }
        });
      } catch (TransactionFailureException e) {
        throw Throwables.propagate(e.getCause() == null ? e : e.getCause());
      }
    }
  }

  public static class NoTxAction extends AbstractCustomAction {

    @Override
    protected void configure() {
      setName(ACTION_NOTX);
    }

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    protected void initialize() throws Exception {
      recordTransaction(getContext(), ACTION_NOTX, INITIALIZE);
      executeRecordTransaction(getContext(), ACTION_NOTX, INITIALIZE_TX, TIMEOUT_ACTION_INITIALIZE);
      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext ctext) throws Exception {
          attemptNestedTransaction(getContext(), ACTION_NOTX, INITIALIZE_NEST);
        }
      });
    }

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public void destroy() {
      recordTransaction(getContext(), ACTION_NOTX, DESTROY);
      executeRecordTransaction(getContext(), ACTION_NOTX, DESTROY_TX, TIMEOUT_ACTION_DESTROY);
      try {
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext ctext) throws Exception {
            attemptNestedTransaction(getContext(), ACTION_NOTX, DESTROY_NEST);
          }
        });
      } catch (TransactionFailureException e) {
        throw Throwables.propagate(e.getCause() == null ? e : e.getCause());
      }
    }

    @Override
    public void run() throws Exception {
      recordTransaction(getContext(), ACTION_NOTX, RUNTIME_TX);
      executeRecordTransaction(getContext(), ACTION_NOTX, RUNTIME_TX, TIMEOUT_ACTION_RUNTIME);
      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext ctext) throws Exception {
          attemptNestedTransaction(getContext(), ACTION_NOTX, RUNTIME_NEST);
        }
      });
    }
  }

  public static class TxAction extends AbstractCustomAction {

    @Override
    protected void configure() {
      setName(ACTION_TX);
    }

    @Override
    protected void initialize() throws Exception {
      recordTransaction(getContext(), ACTION_TX, INITIALIZE);
      attemptNestedTransaction(getContext(), ACTION_TX, INITIALIZE_NEST);
    }

    @Override
    public void destroy() {
      recordTransaction(getContext(), ACTION_TX, DESTROY);
      attemptNestedTransaction(getContext(), ACTION_TX, DESTROY_NEST);
    }

    @Override
    public void run() throws Exception {
      recordTransaction(getContext(), ACTION_TX, RUNTIME_TX);
      executeRecordTransaction(getContext(), ACTION_TX, RUNTIME_TX, TIMEOUT_ACTION_RUNTIME);
      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext ctext) throws Exception {
          attemptNestedTransaction(getContext(), ACTION_TX, RUNTIME_NEST);
        }
      });
    }
  }

  static class TxFlowlet extends AbstractFlowlet {

    @SuppressWarnings("unused")
    private OutputEmitter<StreamEvent> out;

    @Override
    protected void configure() {
      setName(FLOWLET_TX);
    }

    @Override
    public void initialize(FlowletContext context) throws Exception {
      super.initialize(context);
      recordTransaction(context, context.getName(), INITIALIZE);
      attemptNestedTransaction(context, context.getName(), INITIALIZE_NEST);
    }

    @Override
    public void destroy() {
      recordTransaction(getContext(), getContext().getName(), DESTROY);
      attemptNestedTransaction(getContext(), getContext().getName(), DESTROY_NEST);
    }

    @ProcessInput
    public void process(StreamEvent event) {
      recordTransaction(getContext(), getContext().getName(), RUNTIME);
      attemptNestedTransaction(getContext(), getContext().getName(), RUNTIME_NEST);
      out.emit(event);
    }
  }

  public static class NoTxFlowlet extends AbstractFlowlet {

    @Override
    protected void configure() {
      setName(FLOWLET_NOTX);
    }

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public void initialize(final FlowletContext context) throws Exception {
      super.initialize(context);
      recordTransaction(context, context.getName(), INITIALIZE);
      executeRecordTransaction(context, context.getName(), INITIALIZE_TX, TIMEOUT_FLOWLET_INITIALIZE);
      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext ctext) throws Exception {
          attemptNestedTransaction(context, context.getName(), INITIALIZE_NEST);
        }
      });
    }

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public void destroy() {
      recordTransaction(getContext(), getContext().getName(), DESTROY);
      executeRecordTransaction(getContext(), getContext().getName(), DESTROY_TX, TIMEOUT_FLOWLET_DESTROY);
      try {
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext ctext) throws Exception {
            attemptNestedTransaction(getContext(), getContext().getName(), DESTROY_NEST);
          }
        });
      } catch (TransactionFailureException e) {
        throw Throwables.propagate(e.getCause() == null ? e : e.getCause());
      }
    }

    @ProcessInput
    public void process(@SuppressWarnings("UnusedParameters") StreamEvent event) {
      // no-op
    }
  }

  public static class NoTxMR extends AbstractMapReduce {
    @Override
    protected void configure() {
      setName(MAPREDUCE_NOTX);
    }

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    protected void initialize() throws Exception {
      // this job will fail because we don't configure the mapper etc. That is fine because destroy() still gets called
      recordTransaction(getContext(), MAPREDUCE_NOTX, INITIALIZE);
      executeRecordTransaction(getContext(), MAPREDUCE_NOTX, INITIALIZE_TX, TIMEOUT_MAPREDUCE_INITIALIZE);
      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext ctext) throws Exception {
          attemptNestedTransaction(getContext(), MAPREDUCE_NOTX, INITIALIZE_NEST);
        }
      });

      // TODO (CDAP-7444): if destroy is called if the MR fails to start, we can remove all this to speed up the test
      Job job = getContext().getHadoopJob();
      job.setMapperClass(NoOpMapper.class);
      job.setNumReduceTasks(0);
      job.setInputFormatClass(SingleRecordInputFormat.class);
      job.setOutputFormatClass(NoOpOutputFormat.class);
    }

    @Override
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public void destroy() {
      recordTransaction(getContext(), MAPREDUCE_NOTX, DESTROY);
      executeRecordTransaction(getContext(), MAPREDUCE_NOTX, DESTROY_TX, TIMEOUT_MAPREDUCE_DESTROY);
      try {
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext ctext) throws Exception {
            attemptNestedTransaction(getContext(), MAPREDUCE_NOTX, DESTROY_NEST);
          }
        });
      } catch (TransactionFailureException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  public static class TxMR extends AbstractMapReduce {
    @Override
    protected void configure() {
      setName(MAPREDUCE_TX);
    }

    @Override
    protected void initialize() throws Exception {
      // this job will fail because we don't configure the mapper etc. That is fine because destroy() still gets called
      recordTransaction(getContext(), MAPREDUCE_TX, INITIALIZE);
      attemptNestedTransaction(getContext(), MAPREDUCE_TX, INITIALIZE_NEST);

      // TODO (CDAP-7444): if destroy is called if the MR fails to start, we can remove all this to speed up the test
      Job job = getContext().getHadoopJob();
      job.setMapperClass(NoOpMapper.class);
      job.setNumReduceTasks(0);
      job.setInputFormatClass(SingleRecordInputFormat.class);
      job.setOutputFormatClass(NoOpOutputFormat.class);
    }

    @Override
    public void destroy() {
      recordTransaction(getContext(), MAPREDUCE_TX, DESTROY);
      attemptNestedTransaction(getContext(), MAPREDUCE_TX, DESTROY_NEST);
    }
  }

  public static class NoOpMapper extends Mapper<Void, Void, Void, Void> {
    @Override
    protected void map(Void key, Void value, Context context) throws IOException, InterruptedException {
      // no-op
    }
  }

  public static class SingleRecordSplit extends InputSplit implements Writable {
    @Override
    public long getLength() throws IOException, InterruptedException {
      return 1;
    }
    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return new String[0];
    }
    @Override
    public void write(DataOutput out) throws IOException {
    }
    @Override
    public void readFields(DataInput in) throws IOException {
    }
  }

  private static class SingleRecordInputFormat extends InputFormat {
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
      return Collections.<InputSplit>singletonList(new SingleRecordSplit());
    }
    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
      return new RecordReader<Void, Void>() {
        private int count = 0;
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        }
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
          return count++ == 0;
        }
        @Override
        public Void getCurrentKey() throws IOException, InterruptedException {
          return null;
        }
        @Override
        public Void getCurrentValue() throws IOException, InterruptedException {
          return null;
        }
        @Override
        public float getProgress() throws IOException, InterruptedException {
          return count;
        }
        @Override
        public void close() throws IOException {
        }
      };
    }
  }

  private static class NoOpOutputFormat extends OutputFormat<Void, Void> {
    @Override
    public RecordWriter<Void, Void> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
      return new RecordWriter<Void, Void>() {
        @Override
        public void write(Void key, Void value) throws IOException, InterruptedException {
        }
        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        }
      };
    }
    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    }
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException, InterruptedException {
      return new OutputCommitter() {
        @Override
        public void setupJob(JobContext jobContext) throws IOException {
        }
        @Override
        public void setupTask(TaskAttemptContext taskContext) throws IOException {
        }
        @Override
        public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
          return false;
        }
        @Override
        public void commitTask(TaskAttemptContext taskContext) throws IOException {
        }
        @Override
        public void abortTask(TaskAttemptContext taskContext) throws IOException {
        }
      };
    }
  }
}
