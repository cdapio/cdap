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

import co.cask.cdap.api.data.batch.DatasetOutputCommitter;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.data2.transaction.RetryingLongTransactionSystemClient;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.runtime.batch.dataset.output.MultipleOutputsCommitter;
import co.cask.cdap.internal.app.runtime.batch.dataset.output.Outputs;
import co.cask.cdap.internal.app.runtime.batch.dataset.output.ProvidedOutput;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * An {@link OutputCommitter} used to start/finish the long transaction used by the MapReduce job.
 * Also responsible for executing {@link DatasetOutputCommitter} methods for any such outputs.
 */
public class MainOutputCommitter extends MultipleOutputsCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(MainOutputCommitter.class);

  private final TaskAttemptContext taskAttemptContext;

  private CConfiguration cConf;
  private TransactionSystemClient txClient;
  private Transaction transaction;
  private BasicMapReduceTaskContext taskContext;
  private List<ProvidedOutput> outputs;
  private boolean completedCallingOnFailure;

  public MainOutputCommitter(OutputCommitter rootOutputCommitter, Map<String, OutputCommitter> committers,
                             TaskAttemptContext taskAttemptContext) {
    super(rootOutputCommitter, committers);
    this.taskAttemptContext = taskAttemptContext;
  }

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    Configuration configuration = jobContext.getConfiguration();
    MapReduceClassLoader classLoader = MapReduceClassLoader.getFromConfiguration(configuration);
    MapReduceTaskContextProvider taskContextProvider = classLoader.getTaskContextProvider();
    Injector injector = taskContextProvider.getInjector();

    cConf = injector.getInstance(CConfiguration.class);
    MapReduceContextConfig contextConfig = new MapReduceContextConfig(jobContext.getConfiguration());

    ProgramId programId = contextConfig.getProgramId();
    LOG.info("Setting up for MapReduce job: namespaceId={}, applicationId={}, program={}, runid={}",
             programId.getNamespace(), programId.getApplication(), programId.getProgram(),
             ProgramRunners.getRunId(contextConfig.getProgramOptions()));

    RetryStrategy retryStrategy =
      SystemArguments.getRetryStrategy(contextConfig.getProgramOptions().getUserArguments().asMap(),
                                       contextConfig.getProgramId().getType(), cConf);
    this.txClient = new RetryingLongTransactionSystemClient(injector.getInstance(TransactionSystemClient.class),
                                                            retryStrategy);

    // We start long-running tx to be used by mapreduce job tasks.
    //this.transaction = txClient.startLong();

    // Write the tx somewhere, so that we can re-use it in mapreduce tasks
    Path txFile = getTxFile(configuration, jobContext.getJobID());
    FileSystem fs = txFile.getFileSystem(configuration);
    try (FSDataOutputStream fsDataOutputStream = fs.create(txFile, false)) {
      //fsDataOutputStream.write(new TransactionCodec().encode(transaction));
    }

    // we can instantiate the TaskContext after we set the tx above. It's used by the operations below
    taskContext = taskContextProvider.get(taskAttemptContext);
    this.outputs = Outputs.transform(contextConfig.getOutputs(), taskContext);

    super.setupJob(jobContext);
  }

  // returns a Path to a file in the staging directory of the MapReduce
  static Path getTxFile(Configuration configuration, @Nullable JobID jobID) throws IOException {
    Path stagingDir = MRApps.getStagingAreaDir(configuration, UserGroupInformation.getCurrentUser().getShortUserName());
    int appAttemptId = configuration.getInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
    // following the pattern of other files in the staging dir
    String jobId = jobID != null ? jobID.toString() : configuration.get(MRJobConfig.ID);
    return new Path(stagingDir, jobId + "/"  + jobId + "_" + appAttemptId + "_tx-file");
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    super.commitJob(jobContext);

    onFinish(jobContext, true);
    commitTx();
  }

  private void commitTx() throws IOException {
      if (transaction == null) {
          return;
      }
    try {
      LOG.debug("Committing MapReduce Job transaction: {}", transaction.getWritePointer());

      // "Commit" the data event topic by publishing an empty message.
      // Need to do it with the raw MessagingService.
      taskContext.getMessagingService().publish(
        StoreRequestBuilder
          .of(NamespaceId.SYSTEM.topic(cConf.get(Constants.Dataset.DATA_EVENT_TOPIC)))
          .setTransaction(transaction.getWritePointer())
          .build());

      // flush dataset operations (such as from any DatasetOutputCommitters)
      taskContext.flushOperations();
      // no need to rollback changes if commit fails, as these changes where performed by mapreduce tasks
      // NOTE: can't call afterCommit on datasets in this case: the changes were made by external processes.
      //try {
        //txClient.commitOrThrow(transaction);
      //} catch (TransactionFailureException e) {
      //  LOG.warn("MapReduce Job transaction {} failed to commit", transaction.getTransactionId());
      //  throw e;
      //}
      taskContext.postTxCommit();
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, IOException.class);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
    try {
      super.abortJob(jobContext, state);
    } finally {
      try {
        onFinish(jobContext, false);
      } finally {
        if (transaction != null) {
          // invalids long running tx. All writes done by MR cannot be undone at this point.
          LOG.info("Invalidating transaction {}", transaction.getWritePointer());
          txClient.invalidate(transaction.getWritePointer());
          transaction = null;
        } else {
          LOG.warn("Did not invalidate transaction; job setup did not complete or invalidate already happened.");
        }
      }
    }
  }

  private void onFinish(JobContext jobContext, boolean success) throws IOException {
    try {
      if (taskContext == null || outputs == null) {
        // taskContext or outputs can only be null if #setupJob fails
        LOG.warn("Did not commit datasets, since job setup did not complete.");
        return;
      }
      finishDatasets(jobContext, success);
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, IOException.class);
      Throwables.propagate(e);
    }
  }

  // returns alias-->DatasetOutputCommitter mapping, given the ProvidedOutputs
  private Map<String, DatasetOutputCommitter> getDatasetOutputCommitters(List<ProvidedOutput> providedOutputs) {
    Map<String, DatasetOutputCommitter> datasetOutputCommitterOutputs = new HashMap<>();
    for (ProvidedOutput providedOutput : providedOutputs) {
      if (providedOutput.getOutputFormatProvider() instanceof DatasetOutputCommitter) {
        datasetOutputCommitterOutputs.put(providedOutput.getOutput().getAlias(),
                                         (DatasetOutputCommitter) providedOutput.getOutputFormatProvider());
      }
    }
    return datasetOutputCommitterOutputs;
  }

  /**
   * Either calls onSuccess or onFailure on all of the DatasetOutputCommitters.
   */
  private void finishDatasets(final JobContext jobContext, final boolean success) throws Exception {
    ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(jobContext.getConfiguration().getClassLoader());
    Map<String, DatasetOutputCommitter> datasetOutputCommitters = getDatasetOutputCommitters(outputs);
    try {
      if (success) {
        commitOutputs(datasetOutputCommitters);
      } else {
        // but this output committer failed: call onFailure() for all committers
        failOutputs(datasetOutputCommitters);
      }
    } finally {
      ClassLoaders.setContextClassLoader(oldClassLoader);
    }
  }

  /**
   * Calls onSuccess on the DatasetOutputCommitters. If any throws an exception, the exception is propagated
   * immediately. #abortJob will then be called, which is responsible for calling onFinish on each of them.
   */
  private void commitOutputs(Map<String, DatasetOutputCommitter> datasetOutputCommiters) {
    for (Map.Entry<String, DatasetOutputCommitter> datasetOutputCommitter : datasetOutputCommiters.entrySet()) {
      try {
        datasetOutputCommitter.getValue().onSuccess();
      } catch (Exception e) {
        LOG.error(String.format("Error from onSuccess method of output dataset %s.",
                                datasetOutputCommitter.getKey()), e);
        throw e;
      }
    }
  }

  /**
   * Calls onFailure for all of the DatasetOutputCommitters. If any operation throws an exception, it is suppressed
   * until onFailure is called on all of the DatasetOutputCommitters.
   */
  private void failOutputs(Map<String, DatasetOutputCommitter> datasetOutputCommiters) throws Exception {
    if (completedCallingOnFailure) {
      // guard against multiple calls to OutputCommitter#abortJob
      LOG.info("Not calling onFailure on outputs, as it has they have already been executed.");
      return;
    }
    Exception e = null;
    for (Map.Entry<String, DatasetOutputCommitter> datasetOutputCommitter : datasetOutputCommiters.entrySet()) {
      try {
        datasetOutputCommitter.getValue().onFailure();
      } catch (Exception suppressedException) {
        LOG.error(String.format("Error from onFailure method of output dataset %s.",
                                datasetOutputCommitter.getKey()), suppressedException);
        if (e != null) {
          e.addSuppressed(suppressedException);
        } else {
          e = suppressedException;
        }
      }
    }
    completedCallingOnFailure = true;
    if (e != null) {
      throw e;
    }
  }

}
