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

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.batch.DatasetOutputCommitter;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.data2.transaction.RetryingLongTransactionSystemClient;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.runtime.batch.dataset.output.MultipleOutputsCommitter;
import co.cask.cdap.internal.app.runtime.batch.dataset.output.Outputs;
import co.cask.cdap.internal.app.runtime.batch.dataset.output.ProvidedOutput;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionCodec;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An {@link OutputCommitter} used to
 */
public class MainOutputCommitter extends MultipleOutputsCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(MainOutputCommitter.class);

  private CConfiguration cConf;
  private TransactionSystemClient txClient;
  private Transaction transaction;
  private BasicMapReduceTaskContext taskContext;

  private List<ProvidedOutput> outputs;
  private boolean success;

  public MainOutputCommitter(TaskAttemptContext taskAttemptContext) {
    super(taskAttemptContext);
  }

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    Configuration configuration = jobContext.getConfiguration();
    MapReduceClassLoader classLoader = MapReduceClassLoader.getFromConfiguration(configuration);
    MapReduceTaskContextProvider taskContextProvider = classLoader.getTaskContextProvider();
    Injector injector = taskContextProvider.getInjector();

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
      this.transaction = tx;
    } catch (Throwable t) {
      Transactions.invalidateQuietly(txClient, tx);
      throw t;
    }

    // Write the tx somewhere, so that we can re-use it in mapreduce tasks
    // we can't piggy back on MapReduce staging directory, since its only there for FileOutputFormat
    // (whose FileOutputCommitter#setupJob hasn't run yet)
    Path txFile = getTxFile(configuration, jobContext.getJobID());
    FileSystem fs = txFile.getFileSystem(jobContext.getConfiguration());
    try (FSDataOutputStream fsDataOutputStream = fs.create(txFile, false)) {
      fsDataOutputStream.write(new TransactionCodec().encode(transaction));
    }

    // we can instantiate the TaskContext after we set the tx above. It's used by the operations below
    taskContext = taskContextProvider.get(taskAttemptContext);
    this.outputs = Outputs.transform(contextConfig.getOutputs(), taskContext);

    super.setupJob(jobContext);
  }

  // returns a Path to a file in the staging directory of the MapReduce
  static Path getTxFile(Configuration configuration, JobID jobId) throws IOException {
    Path stagingDir = MRApps.getStagingAreaDir(configuration, UserGroupInformation.getCurrentUser().getShortUserName());
    int appAttemptId = configuration.getInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
    // following the pattern of other files in the staging dir
    return new Path(stagingDir, jobId.toString() + "/"  + jobId.toString() + "_" + appAttemptId + "_tx-file");
  }

  /**
   * Commit a single output after the MR has finished, if it is an OutputFormatCommitter.
   * If any exception is thrown by the output committer, sets the failure cause to that exception.
   * @param output configured ProvidedOutput
   */
  private void commitOutput(ProvidedOutput output, AtomicReference<Exception> failureCause) {
    OutputFormatProvider outputFormatProvider = output.getOutputFormatProvider();
    if (outputFormatProvider instanceof DatasetOutputCommitter) {
      try {
        if (success) {
          ((DatasetOutputCommitter) outputFormatProvider).onSuccess();
        } else {
          ((DatasetOutputCommitter) outputFormatProvider).onFailure();
        }
      } catch (Throwable t) {
        LOG.error(String.format("Error from %s method of output dataset %s.",
                                success ? "onSuccess" : "onFailure", output.getOutput().getAlias()), t);
        success = false;
        if (failureCause.get() != null) {
          failureCause.get().addSuppressed(t);
        } else {
          failureCause.set(t instanceof Exception ? (Exception) t : new RuntimeException(t));
        }
      }
    }
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    super.commitJob(jobContext);
    success = true;
    // commitTx
    doShutdown(jobContext);
  }

  @Override
  public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
    // TODO: handle being called multiple times
    super.abortJob(jobContext, state);

    // invalidate tx
    doShutdown(jobContext);
  }

  private void doShutdown(JobContext jobContext) throws IOException {
    try {
      try {
        shutdown(jobContext);
      } finally {
        if (taskContext != null) {
          commitDatasets(jobContext);
        } else {
          // taskContext can only be null if #setupJob fails
          LOG.warn("Did not commit datasets, since job setup did not complete.");
        }
      }
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, IOException.class);
      Throwables.propagate(e);
    }
  }

  private void shutdown(JobContext jobContext) throws Exception {
    if (success) {
      LOG.debug("Committing MapReduce Job transaction: {}", toString(jobContext));

      // "Commit" the data event topic by publishing an empty message.
      // Need to do it with the raw MessagingService.
      taskContext.getMessagingService().publish(
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

  private List<ProvidedOutput> getOutputs() {
    return Preconditions.checkNotNull(outputs);
  }

  private void commitDatasets(final JobContext jobContext) throws Exception {
    final AtomicReference<Exception> failureCause = new AtomicReference<>();

    try {
      taskContext.execute(new TxRunnable() {
        @Override
        public void run(DatasetContext ctxt) throws Exception {
          ClassLoader oldClassLoader =
            ClassLoaders.setContextClassLoader(jobContext.getConfiguration().getClassLoader());
          try {
            for (ProvidedOutput output : getOutputs()) {
              boolean wasSuccess = success;
              commitOutput(output, failureCause);
              if (wasSuccess && !success) {
                // mapreduce was successful but this output committer failed: call onFailure() for all committers
                for (ProvidedOutput toFail : getOutputs()) {
                  commitOutput(output, failureCause);
                }
                break;
              }
            }
            // if there was a failure, we must throw an exception to fail the transaction
            // this will roll back all the outputs and also make sure that postCommit() is not called
            // throwing the failure cause: it will be wrapped in a TxFailure and handled in the outer catch()
            Exception cause = failureCause.get();
            if (cause != null) {
              failureCause.set(null);
              throw cause;
            }
          } finally {
            ClassLoaders.setContextClassLoader(oldClassLoader);
          }
        }
      });
    } catch (TransactionFailureException e) {
      LOG.error("Transaction failure when committing dataset outputs", e);
      if (failureCause.get() != null) {
        failureCause.get().addSuppressed(e);
      } else {
        failureCause.set(e);
      }
    }

    if (failureCause.get() != null) {
      throw failureCause.get();
    }
  }
}
