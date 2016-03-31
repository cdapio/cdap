/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.TaskLocalizationContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.BatchWritable;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.metrics.MultiMetricsContext;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.metrics.MapReduceMetrics;
import co.cask.cdap.app.metrics.ProgramUserMetrics;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.cdap.internal.app.runtime.DefaultTaskLocalizationContext;
import co.cask.cdap.internal.app.runtime.batch.dataset.CloseableBatchWritable;
import co.cask.cdap.internal.app.runtime.batch.dataset.ForwardingSplitReader;
import co.cask.cdap.internal.app.runtime.batch.dataset.MultipleOutputs;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Mapreduce task runtime context which delegates to BasicMapReduceContext for non task-specific methods.
 * It currently also extends MapReduceContext to support backwards compatibility. Mapper and Reducer tasks could
 * implement ProgramLifeCycle<MapReduceContext> in order to to get a MapReduceContext object.
 *
 * @param <KEYOUT>   output key type
 * @param <VALUEOUT> output value type
 */
public class BasicMapReduceTaskContext<KEYOUT, VALUEOUT> extends AbstractContext
  implements MapReduceTaskContext<KEYOUT, VALUEOUT> {

  private final MapReduceSpecification spec;
  private final WorkflowProgramInfo workflowProgramInfo;
  private final Metrics userMetrics;
  private final Map<String, Plugin> plugins;
  private final Transaction transaction;
  private final TaskLocalizationContext taskLocalizationContext;

  private MultipleOutputs multipleOutputs;
  private TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> context;
  private String inputName;

  // keeps track of all tx-aware datasets to perform the transaction lifecycle for them. Note that
  // the transaction is already started, and it will be committed or aborted outside of this task.
  // TODO: (CDAP-3983) The datasets should be managed by the dataset context. That requires TEPHRA-99.
  private final Set<TransactionAware> txAwares = Sets.newIdentityHashSet();

  public BasicMapReduceTaskContext(Program program,
                                   @Nullable MapReduceMetrics.TaskType type,
                                   RunId runId, String taskId,
                                   Arguments runtimeArguments,
                                   MapReduceSpecification spec,
                                   @Nullable WorkflowProgramInfo workflowProgramInfo,
                                   DiscoveryServiceClient discoveryServiceClient,
                                   MetricsCollectionService metricsCollectionService,
                                   TransactionSystemClient txClient,
                                   Transaction transaction,
                                   DatasetFramework dsFramework,
                                   @Nullable PluginInstantiator pluginInstantiator,
                                   Map<String, File> localizedResources) {
    super(program, runId, runtimeArguments, ImmutableSet.<String>of(),
          createMetricsContext(program, runId.getId(), metricsCollectionService, taskId, type, workflowProgramInfo),
          dsFramework, txClient, discoveryServiceClient, false, pluginInstantiator);
    this.workflowProgramInfo = workflowProgramInfo;
    this.transaction = transaction;
    this.userMetrics = new ProgramUserMetrics(getProgramMetrics());
    this.spec = spec;
    this.plugins = Maps.newHashMap(program.getApplicationSpecification().getPlugins());
    this.taskLocalizationContext = new DefaultTaskLocalizationContext(localizedResources);

    initializeTransactionAwares();
  }

  @Override
  public String toString() {
    return String.format("job=%s,=%s", spec.getName(), super.toString());
  }

  @Override
  public Map<String, Plugin> getPlugins() {
    return plugins;
  }

  @Override
  public <K, V> void write(String namedOutput, K key, V value) throws IOException, InterruptedException {
    if (multipleOutputs == null) {
      throw new IOException("MultipleOutputs has not been initialized.");
    }
    multipleOutputs.write(namedOutput, key, value);
  }

  @Override
  public void write(KEYOUT key, VALUEOUT value) throws IOException, InterruptedException {
    if (multipleOutputs == null) {
      throw new IOException("Hadoop context has not been initialized.");
    }
    context.write(key, value);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getHadoopContext() {
    return (T) context;
  }

  public void setHadoopContext(TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> context) {
    this.multipleOutputs = new MultipleOutputs(context);
    this.context = context;
  }

  public void setInputName(String inputName) {
    this.inputName = inputName;
  }

  /**
   * Closes the {@link MultipleOutputs} contained inside this context.
   */
  public void closeMultiOutputs() {
    if (multipleOutputs != null) {
      multipleOutputs.close();
    }
  }

  @Override
  public MapReduceSpecification getSpecification() {
    return spec;
  }

  /**
   * Returns the WorkflowToken if the MapReduce program is executed as a part of the Workflow.
   */
  @Override
  @Nullable
  public WorkflowToken getWorkflowToken() {
    return workflowProgramInfo == null ? null : workflowProgramInfo.getWorkflowToken();
  }

  @Nullable
  @Override
  public WorkflowProgramInfo getWorkflowInfo() {
    return workflowProgramInfo;
  }

  @Nullable
  @Override
  public String getInputName() {
    return inputName;
  }

  private static MetricsContext createMetricsContext(Program program, String runId, MetricsCollectionService service,
                                                     String taskId, @Nullable MapReduceMetrics.TaskType type,
                                                     @Nullable WorkflowProgramInfo workflowProgramInfo) {
    Map<String, String> tags = Maps.newHashMap();
    tags.putAll(getMetricsContext(program, runId));
    if (type != null) {
      tags.put(Constants.Metrics.Tag.MR_TASK_TYPE, type.getId());
      tags.put(Constants.Metrics.Tag.INSTANCE_ID, taskId);
    }

    MetricsContext programMetricsContext = service.getContext(tags);

    if (workflowProgramInfo == null) {
      return programMetricsContext;
    }

    // If running inside Workflow, add the WorkflowMetricsContext as well
    tags = Maps.newHashMap();
    tags.put(Constants.Metrics.Tag.NAMESPACE, program.getNamespaceId());
    tags.put(Constants.Metrics.Tag.APP, program.getApplicationId());
    tags.put(Constants.Metrics.Tag.WORKFLOW, workflowProgramInfo.getName());
    tags.put(Constants.Metrics.Tag.RUN_ID, workflowProgramInfo.getRunId().getId());
    tags.put(Constants.Metrics.Tag.NODE, workflowProgramInfo.getNodeId());

    return new MultiMetricsContext(programMetricsContext, service.getContext(tags));
  }

  @Override
  public Metrics getMetrics() {
    return userMetrics;
  }

  //---- following are methods to manage transaction lifecycle for the datasets. This needs to
  //---- be refactored after [TEPHRA-99] and [CDAP-3893] are resolved.

  /**
   * Initializes the transaction-awares to be only the static datasets.
   */
  private void initializeTransactionAwares() {
    for (TransactionAware txAware : getDatasetCache().getStaticTransactionAwares()) {
      txAwares.add(txAware);
      txAware.startTx(transaction);
    }
  }

  /**
   * This delegates the instantiation of the dataset to the super class, but in addition, if
   * the dataset is a new transaction-aware, it starts the transaction and remembers the dataset.
   */
  @Override
  public <T extends Dataset> T getDataset(String name, Map<String, String> arguments)
    throws DatasetInstantiationException {
    return getDataset(name, arguments, AccessType.UNKNOWN);
  }

  protected <T extends Dataset> T getDataset(String name, Map<String, String> arguments, AccessType accessType)
    throws DatasetInstantiationException {
    T dataset = super.getDataset(name, arguments, accessType);
    if (dataset instanceof TransactionAware) {
      TransactionAware txAware = (TransactionAware) dataset;
      if (txAwares.add(txAware)) {
        txAware.startTx(transaction);
      }
    }
    return dataset;
  }

  @Override
  public void releaseDataset(Dataset dataset) {
    // nop-op: all datasets have to participate until the transaction (that is, the program) finishes
  }

  @Override
  public void discardDataset(Dataset dataset) {
    // nop-op: all datasets have to participate until the transaction (that is, the program) finishes
  }

  /**
   * Force all transaction-aware datasets participating in this context to flush their writes.
   */
  public void flushOperations() throws Exception {
    for (TransactionAware txAware : txAwares) {
      txAware.commitTx();
    }
  }

  @Override
  public File getLocalFile(String name) throws FileNotFoundException {
    return taskLocalizationContext.getLocalFile(name);
  }

  @Override
  public Map<String, File> getAllLocalFiles() {
    return taskLocalizationContext.getAllLocalFiles();
  }

  /**
   * Returns a {@link BatchReadable} that reads data from the given dataset.
   */
  <K, V> BatchReadable<K, V> getBatchReadable(String datasetName, Map<String, String> datasetArgs) {
    Dataset dataset = getDataset(datasetName, datasetArgs, AccessType.READ);
    // Must be BatchReadable.
    Preconditions.checkArgument(dataset instanceof BatchReadable, "Dataset '%s' is not a BatchReadable.", datasetName);

    @SuppressWarnings("unchecked")
    final BatchReadable<K, V> delegate = (BatchReadable<K, V>) dataset;
    return new BatchReadable<K, V>() {
      @Override
      public List<Split> getSplits() {
        try {
          try {
            return delegate.getSplits();
          } finally {
            flushOperations();
          }
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public SplitReader<K, V> createSplitReader(Split split) {
        return new ForwardingSplitReader<K, V>(delegate.createSplitReader(split)) {
          @Override
          public void close() {
            try {
              try {
                super.close();
              } finally {
                flushOperations();
              }
            } catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }
        };
      }
    };
  }

  /**
   * Returns a {@link CloseableBatchWritable} that writes data to the given dataset.
   */
  <K, V> CloseableBatchWritable<K, V> getBatchWritable(String datasetName, Map<String, String> datasetArgs) {
    Dataset dataset = getDataset(datasetName, datasetArgs, AccessType.WRITE);
    // Must be BatchWritable.
    Preconditions.checkArgument(dataset instanceof BatchWritable, "Dataset '%s' is not a BatchWritable.", datasetName);

    @SuppressWarnings("unchecked") final
    BatchWritable<K, V> delegate = (BatchWritable<K, V>) dataset;
    return new CloseableBatchWritable<K, V>() {
      @Override
      public void write(K k, V v) {
        delegate.write(k, v);
      }

      @Override
      public void close() throws IOException {
        try {
          flushOperations();
        } catch (Exception e) {
          Throwables.propagateIfInstanceOf(e, IOException.class);
          throw new IOException(e);
        }
      }
    };
  }
}
