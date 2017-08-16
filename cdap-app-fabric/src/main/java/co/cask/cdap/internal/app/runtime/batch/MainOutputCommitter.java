/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.data2.transaction.RetryingLongTransactionSystemClient;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.runtime.batch.dataset.output.MultipleOutputsCommitter;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * An {@link OutputCommitter} used to
 */
public class MainOutputCommitter extends MultipleOutputsCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(MainOutputCommitter.class);

  private CConfiguration cConf;
  private TransactionSystemClient txClient;
  private Transaction transaction;
  private MessagingService messagingService;

  public MainOutputCommitter(TaskAttemptContext taskAttemptContext) {
    super(taskAttemptContext);
  }

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    // TODO: this is called per job attempt. Is it on the same OutputCommiter object?
    // Similarly, is the abortJob called on the same object?
    // Similarly, is it ok to have a separate tx for each job attempt?

    // start Tx
    Configuration configuration = jobContext.getConfiguration();
    MapReduceClassLoader classLoader = MapReduceClassLoader.getFromConfiguration(configuration);
    MapReduceTaskContextProvider taskContextProvider = classLoader.getTaskContextProvider();
//    BasicMapReduceContext vs BasicMapReduceTaskContext
    Injector injector = taskContextProvider.getInjector();
    messagingService = injector.getInstance(MessagingService.class);

    cConf = injector.getInstance(CConfiguration.class);
    TransactionSystemClient txClient = injector.getInstance(TransactionSystemClient.class);

    MapReduceContextConfig contextConfig = new MapReduceContextConfig(jobContext.getConfiguration());
    RetryStrategy retryStrategy =
      SystemArguments.getRetryStrategy(contextConfig.getProgramOptions().getUserArguments().asMap(),
                                                                       contextConfig.getProgramId().getType(),
                                                                       cConf);
    this.txClient = new RetryingLongTransactionSystemClient(txClient, retryStrategy);

    // We start long-running tx to be used by mapreduce job tasks.
    Transaction tx = txClient.startLong();
    try {
      // We remember tx, so that we can re-use it in mapreduce tasks
      new MapReduceContextConfig(configuration).setTx(tx);
      new MapReduceContextConfig(taskAttemptContext.getConfiguration()).setTx(tx);
      this.transaction = tx;
    } catch (Throwable t) {
      Transactions.invalidateQuietly(txClient, tx);
      throw t;
    }

    super.setupJob(jobContext);
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    super.commitJob(jobContext);
    // commitTx
    doShutdown(jobContext, true);
  }

  @Override
  public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
    // TODO: handle being called multiple times
    super.abortJob(jobContext, state);

    // invalidate tx
    doShutdown(jobContext, false);
  }

  private void doShutdown(JobContext jobContext, boolean success) throws IOException {
    try {
      shutdown(jobContext, success);
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, IOException.class);
      Throwables.propagate(e);
    }
  }

  private void shutdown(JobContext jobContext, boolean success) throws Exception {
    if (success) {
      LOG.debug("Committing MapReduce Job transaction: {}", toString(jobContext));

      // "Commit" the data event topic by publishing an empty message.
      // Need to do it with the raw MessagingService.
      messagingService.publish(
        StoreRequestBuilder
          .of(NamespaceId.SYSTEM.topic(cConf.get(Constants.Dataset.DATA_EVENT_TOPIC)))
          .setTransaction(transaction.getWritePointer())
          .build()
      );

      // committing long running tx: no need to commit datasets, as they were committed in external processes
      // also no need to rollback changes if commit fails, as these changes where performed by mapreduce tasks
      // NOTE: can't call afterCommit on datasets in this case: the changes were made by external processes.
      if (!txClient.commit(transaction)) {
        LOG.warn("MapReduce Job transaction failed to commit");
        throw new TransactionFailureException("Failed to commit transaction for MapReduce " + toString(jobContext));
      }
    } else {
      // invalids long running tx. All writes done by MR cannot be undone at this point.
      txClient.invalidate(transaction.getWritePointer());
    }
  }

  // TODO: rename?
  private String toString(JobContext jobContext) {
    MapReduceContextConfig contextConfig = new MapReduceContextConfig(jobContext.getConfiguration());
    ProgramId programId = contextConfig.getProgramId();
    ProgramRunId runId = programId.run(ProgramRunners.getRunId(contextConfig.getProgramOptions()));
    return String.format("namespaceId=%s, applicationId=%s, program=%s, runid=%s",
                         programId.getNamespace(), programId.getApplication(), programId.getProgram(), runId.getRun());
  }
}
