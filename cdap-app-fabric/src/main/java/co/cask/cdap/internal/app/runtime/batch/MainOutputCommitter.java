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
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.batch.dataset.DatasetOutputFormatProvider;
import co.cask.cdap.internal.app.runtime.batch.dataset.UnsupportedOutputFormat;
import co.cask.cdap.internal.app.runtime.batch.dataset.output.MultipleOutputs;
import co.cask.cdap.internal.app.runtime.batch.dataset.output.MultipleOutputsCommitter;
import co.cask.cdap.internal.app.runtime.batch.dataset.output.MultipleOutputsMainOutputWrapper;
import co.cask.cdap.internal.app.runtime.batch.dataset.output.ProvidedOutput;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An {@link OutputCommitter} used to
 */
public class MainOutputCommitter extends MultipleOutputsCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(MainOutputCommitter.class);

  private CConfiguration cConf;
  private TransactionSystemClient txClient;
  private Transaction transaction;
  private MessagingService messagingService;
  private MapReduceContextConfig contextConfig;
  private BasicMapReduceTaskContext taskContext;

  private Map<String, ProvidedOutput> outputs;
  private boolean success;

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
    txClient = injector.getInstance(TransactionSystemClient.class);
    contextConfig = new MapReduceContextConfig(jobContext.getConfiguration());
    transaction = contextConfig.getTx();

    // we can instantiate the TaskContext after we set the tx above. It's used by the operations below
    taskContext = taskContextProvider.get(taskAttemptContext);


    List<Output> outputs = OutputSerde.getOutputs(configuration);
    Map<String, ProvidedOutput> outputMap = new LinkedHashMap<>(outputs.size());
    for (Output output : outputs) {
      // TODO: remove alias from ProvidedOutput class?
      outputMap.put(output.getAlias(), new ProvidedOutput(output.getAlias(), transform(output)));
    }
    fixOutputPermissions(jobContext, outputMap);
    this.outputs = outputMap;

    setOutputsIfNeeded(jobContext);
    // need this in local mode
    // TODO: do we need to fix output permissions into taskAttemptContext?
    setOutputsIfNeeded(taskAttemptContext);


    super.setupJob(jobContext);
  }


  private OutputFormatProvider transform(Output output) {
    if (output instanceof Output.DatasetOutput) {
      Output.DatasetOutput datasetOutput = (Output.DatasetOutput) output;
      String datasetNamespace = datasetOutput.getNamespace();
      if (datasetNamespace == null) {
        datasetNamespace = taskContext.getNamespace();
      }
      String datasetName = output.getName();
      Map<String, String> args = datasetOutput.getArguments();
      Dataset dataset = taskContext.getDataset(datasetNamespace, datasetName, args, AccessType.WRITE);
      return new DatasetOutputFormatProvider(datasetNamespace, datasetName, args, dataset);

    } else if (output instanceof Output.OutputFormatProviderOutput) {
      return ((Output.OutputFormatProviderOutput) output).getOutputFormatProvider();
    } else {
      // shouldn't happen unless user defines their own Output class
      throw new IllegalArgumentException(String.format("Output %s has unknown output class %s",
                                                       output.getName(), output.getClass().getCanonicalName()));
    }
  }

  private void setOutputsIfNeeded(JobContext jobContext) {
    Map<String, ProvidedOutput> outputMap = getOutputs();
    if (outputMap.isEmpty()) {
      // user is not going through our APIs to add output; leave the job's output format to user
      return;
    }
    OutputFormatProvider rootOutputFormatProvider;
    if (outputMap.size() == 1) {
      // If only one output is configured through the context, then set it as the root OutputFormat
      rootOutputFormatProvider = outputMap.values().iterator().next().getOutputFormatProvider();
    } else {
      // multiple output formats configured via the context. We should use a RecordWriter that doesn't support writing
      // as the root output format in this case to disallow writing directly on the context
      rootOutputFormatProvider =
        new BasicOutputFormatProvider(UnsupportedOutputFormat.class.getName(), new HashMap<String, String>());
    }

    MultipleOutputsMainOutputWrapper.setRootOutputFormat(jobContext,
                                                         rootOutputFormatProvider.getOutputFormatClassName(),
                                                         rootOutputFormatProvider.getOutputFormatConfiguration());

    for (Map.Entry<String, ProvidedOutput> entry : outputMap.entrySet()) {
      ProvidedOutput output = entry.getValue();
      String outputName = output.getAlias();
      String outputFormatClassName = output.getOutputFormatClassName();
      Map<String, String> outputConfig = output.getOutputFormatConfiguration();
      MultipleOutputs.addNamedOutput(jobContext, outputName, outputFormatClassName,
                                     jobContext.getOutputKeyClass(), jobContext.getOutputValueClass(), outputConfig);

    }
  }



  /**
   * Commit a single output after the MR has finished, if it is an OutputFormatCommitter.
   * If any exception is thrown by the output committer, sets the failure cause to that exception.
   * @param outputName the name of the output
   * @param outputFormatProvider the output format provider to commit
   */
  private void commitOutput(String outputName, OutputFormatProvider outputFormatProvider,
                            AtomicReference<Exception> failureCause) {
    if (outputFormatProvider instanceof DatasetOutputCommitter) {
      try {
        if (success) {
          ((DatasetOutputCommitter) outputFormatProvider).onSuccess();
        } else {
          ((DatasetOutputCommitter) outputFormatProvider).onFailure();
        }
      } catch (Throwable t) {
        LOG.error(String.format("Error from %s method of output dataset %s.",
                                success ? "onSuccess" : "onFailure", outputName), t);
        success = false;
        if (failureCause.get() != null) {
          failureCause.get().addSuppressed(t);
        } else {
          failureCause.set(t instanceof Exception ? (Exception) t : new RuntimeException(t));
        }
      }
    }
  }

  private static final String HADOOP_UMASK_PROPERTY = FsPermission.UMASK_LABEL; // fs.permissions.umask-mode

  private void fixOutputPermissions(JobContext job, Map<String, ProvidedOutput> outputs) {
    Configuration jobconf = job.getConfiguration();
    Set<String> outputsWithUmask = new HashSet<>();
    Set<String> outputUmasks = new HashSet<>();
    for (Map.Entry<String, ProvidedOutput> entry : outputs.entrySet()) {
      String umask = entry.getValue().getOutputFormatConfiguration().get(HADOOP_UMASK_PROPERTY);
      if (umask != null) {
        outputsWithUmask.add(entry.getKey());
        outputUmasks.add(umask);
      }
    }
    boolean allOutputsHaveUmask = outputsWithUmask.size() == outputs.size();
    boolean allOutputsAgree = outputUmasks.size() == 1;
    boolean jobConfHasUmask = MapReduceRuntimeService.isProgrammaticConfig(jobconf, HADOOP_UMASK_PROPERTY);
    String jobConfUmask = jobconf.get(HADOOP_UMASK_PROPERTY);

    boolean mustFixUmasks = false;
    if (jobConfHasUmask) {
      // case 1: job conf has a programmatic umask. It prevails.
      mustFixUmasks = !outputsWithUmask.isEmpty();
      if (mustFixUmasks) {
        LOG.info("Overriding permissions of outputs {} because a umask of {} was set programmatically in the job " +
                   "configuration.", outputsWithUmask, jobConfUmask);
      }
    } else if (allOutputsHaveUmask && allOutputsAgree) {
      // case 2: no programmatic umask in job conf, all outputs want the same umask: set it in job conf
      String umaskToUse = outputUmasks.iterator().next();
      jobconf.set(HADOOP_UMASK_PROPERTY, umaskToUse);
      LOG.debug("Setting umask of {} in job configuration because all outputs {} agree on it.",
                umaskToUse, outputsWithUmask);
    } else {
      // case 3: some outputs configure a umask, but not all of them, or not all the same: use job conf default
      mustFixUmasks = !outputsWithUmask.isEmpty();
      if (mustFixUmasks) {
        LOG.warn("Overriding permissions of outputs {} because they configure different permissions. Falling back " +
                   "to default umask of {} in job configuration.", outputsWithUmask, jobConfUmask);
      }
    }

    // fix all output configurations that have a umask by removing that property from their configs
    if (mustFixUmasks) {
      for (String outputName : outputsWithUmask) {
        ProvidedOutput output = outputs.get(outputName);
        Map<String, String> outputConfig = new HashMap<>(output.getOutputFormatConfiguration());
        outputConfig.remove(HADOOP_UMASK_PROPERTY);
        outputs.put(outputName, new ProvidedOutput(output.getAlias(), output.getOutputFormatProvider(),
                                                   output.getOutputFormatClassName(), outputConfig));
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
        shutdown();
      } finally {
        commitDatasets(jobContext);
      }
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, IOException.class);
      Throwables.propagate(e);
    }
  }

  private void shutdown() throws Exception {
    if (success) {
      LOG.debug("Committing MapReduce Job transaction: {}", toString(contextConfig));

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
        throw new TransactionFailureException("Failed to commit transaction for MapReduce " + toString(contextConfig));
      }
    } else {
      // invalids long running tx. All writes done by MR cannot be undone at this point.
      txClient.invalidate(transaction.getWritePointer());
    }
  }

  // TODO: rename?
  public String toString(MapReduceContextConfig contextConfig) {
    ProgramId programId = contextConfig.getProgramId();
    ProgramRunId runId = programId.run(ProgramRunners.getRunId(contextConfig.getProgramOptions()));
    return String.format("namespaceId=%s, applicationId=%s, program=%s, runid=%s",
                         programId.getNamespace(), programId.getApplication(), programId.getProgram(), runId.getRun());
  }

  private Map<String, ProvidedOutput> getOutputs() {
    return Preconditions.checkNotNull(outputs);
  }

    private void commitDatasets(final JobContext jobContext) throws Exception {
      final AtomicReference<Exception> failureCause = new AtomicReference<>();

      // TODO (CDAP-1952): this should be done in the output committer, to make the M/R fail if addPartition fails
      try {
        taskContext.execute(new TxRunnable() {
          @Override
          public void run(DatasetContext ctxt) throws Exception {
            ClassLoader oldClassLoader =
              ClassLoaders.setContextClassLoader(jobContext.getConfiguration().getClassLoader());
            try {
              for (Map.Entry<String, ProvidedOutput> output : getOutputs().entrySet()) {
                boolean wasSuccess = success;
                commitOutput(output.getKey(), output.getValue().getOutputFormatProvider(), failureCause);
                if (wasSuccess && !success) {
                  // mapreduce was successful but this output committer failed: call onFailure() for all committers
                  for (ProvidedOutput toFail : getOutputs().values()) {
                    commitOutput(toFail.getAlias(), toFail.getOutputFormatProvider(), failureCause);
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
