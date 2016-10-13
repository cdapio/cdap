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

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.TaskLocalizationContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.BatchWritable;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.metrics.MapReduceMetrics;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.cdap.internal.app.runtime.DefaultTaskLocalizationContext;
import co.cask.cdap.internal.app.runtime.batch.dataset.CloseableBatchWritable;
import co.cask.cdap.internal.app.runtime.batch.dataset.ForwardingSplitReader;
import co.cask.cdap.internal.app.runtime.batch.dataset.output.MultipleOutputs;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
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

  private final Transaction transaction;
  private final TaskLocalizationContext taskLocalizationContext;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;

  private MultipleOutputs multipleOutputs;
  private TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> context;
  private String inputName;

  // keeps track of all tx-aware datasets to perform the transaction lifecycle for them. Note that
  // the transaction is already started, and it will be committed or aborted outside of this task.
  // TODO: (CDAP-3983) The datasets should be managed by the dataset context. That requires TEPHRA-99.
  private final Set<TransactionAware> txAwares = Sets.newIdentityHashSet();

  // keep track of all ProgramLifeCycle (See PartitionerWrapper or RawComparatorWrapper) registered
  // so that we can call destroy on them
  private final List<ProgramLifecycle> programLifecycles = new ArrayList<>();

  BasicMapReduceTaskContext(Program program, ProgramOptions programOptions,
                            @Nullable MapReduceMetrics.TaskType type, @Nullable String taskId,
                            MapReduceSpecification spec,
                            @Nullable WorkflowProgramInfo workflowProgramInfo,
                            DiscoveryServiceClient discoveryServiceClient,
                            MetricsCollectionService metricsCollectionService,
                            TransactionSystemClient txClient,
                            Transaction transaction,
                            DatasetFramework dsFramework,
                            @Nullable PluginInstantiator pluginInstantiator,
                            Map<String, File> localizedResources,
                            SecureStore secureStore,
                            SecureStoreManager secureStoreManager,
                            AuthorizationEnforcer authorizationEnforcer,
                            AuthenticationContext authenticationContext) {
    super(program, programOptions, ImmutableSet.<String>of(), dsFramework, txClient, discoveryServiceClient, true,
          metricsCollectionService, createMetricsTags(taskId, type, workflowProgramInfo), secureStore,
          secureStoreManager, pluginInstantiator);
    this.workflowProgramInfo = workflowProgramInfo;
    this.transaction = transaction;
    this.spec = spec;
    this.taskLocalizationContext = new DefaultTaskLocalizationContext(localizedResources);
    this.authorizationEnforcer = authorizationEnforcer;
    this.authenticationContext = authenticationContext;
    initializeTransactionAwares();
  }

  Transaction getTransaction() {
    return transaction;
  }

  @Override
  public String toString() {
    return String.format("name=%s,=%s", spec.getName(), super.toString());
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
    // We don't close the multipleOutputs in the close method of this class, because that happens too late.
    // We need to close the multipleOutputs before the OutputCommitter#commitTask is called.
    if (multipleOutputs != null) {
      multipleOutputs.close();
    }
  }

  @Override
  public void close() {
    super.close();

    // destroy all of the ProgramLifeCycles registered
    RuntimeException ex = null;
    for (ProgramLifecycle programLifecycle : programLifecycles) {
      try {
        programLifecycle.destroy();
      } catch (RuntimeException e) {
        if (ex == null) {
          ex = new RuntimeException(e);
        } else {
          ex.addSuppressed(e);
        }
      }
    }
    if (ex != null) {
      throw ex;
    }
  }

  void registerProgramLifecycle(ProgramLifecycle programLifecycle) {
    programLifecycles.add(programLifecycle);
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

  private static Map<String, String> createMetricsTags(@Nullable String taskId,
                                                       @Nullable MapReduceMetrics.TaskType type,
                                                       @Nullable WorkflowProgramInfo workflowProgramInfo) {
    Map<String, String> tags = Maps.newHashMap();
    if (type != null && taskId != null) {
      tags.put(Constants.Metrics.Tag.MR_TASK_TYPE, type.getId());
      tags.put(Constants.Metrics.Tag.INSTANCE_ID, taskId);
    }

    if (workflowProgramInfo != null) {
      workflowProgramInfo.updateMetricsTags(tags);
    }

    return tags;
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

  @Override
  protected <T extends Dataset> T getDataset(String name, Map<String, String> arguments, AccessType accessType)
    throws DatasetInstantiationException {
    T dataset = super.getDataset(name, arguments, accessType);
    startDatasetTransaction(dataset);
    return dataset;
  }

  @Override
  protected <T extends Dataset> T getDataset(String namespace, String name, Map<String, String> arguments,
                                             AccessType accessType) throws DatasetInstantiationException {
    T dataset = super.getDataset(namespace, name, arguments, accessType);
    startDatasetTransaction(dataset);
    return dataset;
  }

  /**
   * If a dataset is a new transaction-aware, it starts the transaction and remembers the dataset.
   */
  private <T extends Dataset> void startDatasetTransaction(T dataset) {
    if (dataset instanceof TransactionAware) {
      TransactionAware txAware = (TransactionAware) dataset;
      if (txAwares.add(txAware)) {
        txAware.startTx(transaction);
      }
    }
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

  /**
   * Return {@link AuthorizationEnforcer} to enforce authorization checks in MR tasks.
   */
  public AuthorizationEnforcer getAuthorizationEnforcer() {
    return authorizationEnforcer;
  }

  /**
   * Return {@link AuthenticationContext} to determine the {@link Principal} for authorization check.
   * */
  public AuthenticationContext getAuthenticationContext() {
    return authenticationContext;
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
  <K, V> BatchReadable<K, V> getBatchReadable(@Nullable String datasetNamespace, String datasetName,
                                              Map<String, String> datasetArgs) {
    Dataset dataset;
    if (datasetNamespace == null) {
      dataset = getDataset(datasetName, datasetArgs, AccessType.READ);
    } else {
      dataset = getDataset(datasetNamespace, datasetName, datasetArgs, AccessType.READ);
    }
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
  <K, V> CloseableBatchWritable<K, V> getBatchWritable(String namespace, String datasetName,
                                                       Map<String, String> datasetArgs) {
    Dataset dataset = getDataset(namespace, datasetName, datasetArgs, AccessType.WRITE);
    // Must be BatchWritable.
    Preconditions.checkArgument(dataset instanceof BatchWritable,
                                "Dataset '%s:%s' is not a BatchWritable.", namespace, datasetName);

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
