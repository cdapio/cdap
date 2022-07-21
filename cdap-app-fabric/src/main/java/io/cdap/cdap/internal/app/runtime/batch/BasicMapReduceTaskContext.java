/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.batch;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.ProgramLifecycle;
import io.cdap.cdap.api.TaskLocalizationContext;
import io.cdap.cdap.api.data.DatasetInstantiationException;
import io.cdap.cdap.api.data.batch.BatchReadable;
import io.cdap.cdap.api.data.batch.BatchWritable;
import io.cdap.cdap.api.data.batch.InputContext;
import io.cdap.cdap.api.data.batch.Split;
import io.cdap.cdap.api.data.batch.SplitReader;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.mapreduce.MapReduceSpecification;
import io.cdap.cdap.api.mapreduce.MapReduceTaskContext;
import io.cdap.cdap.api.messaging.MessageFetcher;
import io.cdap.cdap.api.messaging.MessagePublisher;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.app.metrics.MapReduceMetrics;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.lang.WeakReferenceDelegatorClassLoader;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.lib.table.hbase.HBaseTable;
import io.cdap.cdap.data2.metadata.lineage.AccessType;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.internal.app.runtime.AbstractContext;
import io.cdap.cdap.internal.app.runtime.DefaultTaskLocalizationContext;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.batch.dataset.CloseableBatchWritable;
import io.cdap.cdap.internal.app.runtime.batch.dataset.ForwardingSplitReader;
import io.cdap.cdap.internal.app.runtime.batch.dataset.output.MultipleOutputs;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.client.StoreRequestBuilder;
import io.cdap.cdap.messaging.context.AbstractMessagePublisher;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
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

  private static final Logger LOG = LoggerFactory.getLogger(BasicMapReduceTaskContext.class);

  private final CConfiguration cConf;
  private final MapReduceSpecification spec;
  private final WorkflowProgramInfo workflowProgramInfo;
  private final Transaction transaction;
  private final TaskLocalizationContext taskLocalizationContext;
  private final AccessEnforcer accessEnforcer;
  private final AuthenticationContext authenticationContext;
  private final MapReduceClassLoader mapReduceClassLoader;

  private MultipleOutputs multipleOutputs;
  private TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> context;
  private InputContext inputContext;

  // keeps track of all tx-aware datasets to perform the transaction lifecycle for them. Note that
  // the transaction is already started, and it will be committed or aborted outside of this task.
  // TODO: (CDAP-3983) The datasets should be managed by the dataset context. That requires TEPHRA-99.
  private final Set<TransactionAware> txAwares = Sets.newIdentityHashSet();

  // keep track of all ProgramLifeCycle (See PartitionerWrapper or RawComparatorWrapper) registered
  // so that we can call destroy on them
  private final List<ProgramLifecycle> programLifecycles = new ArrayList<>();

  BasicMapReduceTaskContext(Program program, ProgramOptions programOptions, CConfiguration cConf,
                            @Nullable MapReduceMetrics.TaskType type, @Nullable String taskId,
                            MapReduceSpecification spec,
                            @Nullable WorkflowProgramInfo workflowProgramInfo,
                            DiscoveryServiceClient discoveryServiceClient,
                            MetricsCollectionService metricsCollectionService,
                            TransactionSystemClient txClient,
                            @Nullable Transaction transaction,
                            DatasetFramework dsFramework,
                            @Nullable PluginInstantiator pluginInstantiator,
                            Map<String, File> localizedResources,
                            SecureStore secureStore,
                            SecureStoreManager secureStoreManager,
                            AccessEnforcer accessEnforcer,
                            AuthenticationContext authenticationContext,
                            MessagingService messagingService, MapReduceClassLoader mapReduceClassLoader,
                            MetadataReader metadataReader, MetadataPublisher metadataPublisher,
                            NamespaceQueryAdmin namespaceQueryAdmin, FieldLineageWriter fieldLineageWriter,
                            RemoteClientFactory remoteClientFactory) {
    super(program, programOptions, cConf, ImmutableSet.of(), dsFramework, txClient,
          true, metricsCollectionService, createMetricsTags(programOptions,
                                                            taskId, type, workflowProgramInfo), secureStore,
          secureStoreManager, messagingService, pluginInstantiator, metadataReader, metadataPublisher,
          namespaceQueryAdmin, fieldLineageWriter, remoteClientFactory);
    this.cConf = cConf;
    this.workflowProgramInfo = workflowProgramInfo;
    this.transaction = transaction;
    this.spec = spec;
    this.taskLocalizationContext = new DefaultTaskLocalizationContext(localizedResources);
    this.accessEnforcer = accessEnforcer;
    this.authenticationContext = authenticationContext;
    this.mapReduceClassLoader = mapReduceClassLoader;
    initializeTransactionAwares();
  }

  @Override
  protected ClassLoader createProgramInvocationClassLoader() {
    return new WeakReferenceDelegatorClassLoader(mapReduceClassLoader);
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

  public void setInputContext(InputContext inputContext) {
    this.inputContext = inputContext;
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

  public long getMetricsReportIntervalMillis() {
    return MapReduceMetricsUtil.getReportIntervalMillis(cConf, getRuntimeArguments());
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

  @Override
  public InputContext getInputContext() {
    return inputContext;
  }

  @Override
  protected MessagingContext getMessagingContext() {
    // Override to have transactional publisher use "store" instead of "payload"
    // since a task is executed with long transaction.
    // The actual publish will be done in the MR driver.
    final MessagingContext context = super.getMessagingContext();

    // TODO: CDAP-7807 Make it available for any topic
    final TopicId allowedTopic = NamespaceId.SYSTEM.topic(cConf.get(Constants.Dataset.DATA_EVENT_TOPIC));

    return new MessagingContext() {
      @Override
      public MessagePublisher getMessagePublisher() {
        return new AbstractMessagePublisher() {
          @Override
          protected void publish(TopicId topicId,
                                 Iterator<byte[]> payloads)
            throws IOException, TopicNotFoundException, UnauthorizedException {
            if (!allowedTopic.equals(topicId)) {
              throw new UnsupportedOperationException("Publish to topic '" + topicId.getTopic() + "' is not supported");
            }

            // Use MessagingService directly so that we can publish to system namespace
            // If there is transaction, use storePayload. Otherwise publish without transaction
            if (transaction == null) {
              getMessagingService().publish(StoreRequestBuilder.of(topicId).addPayloads(payloads).build());
            } else {
              getMessagingService().storePayload(StoreRequestBuilder.of(topicId)
                                                   .setTransaction(transaction.getWritePointer())
                                                   .addPayloads(payloads).build());
            }
          }
        };
      }

      @Override
      public MessagePublisher getDirectMessagePublisher() {
        return context.getDirectMessagePublisher();
      }

      @Override
      public MessageFetcher getMessageFetcher() {
        return context.getMessageFetcher();
      }
    };
  }

  private static Map<String, String> createMetricsTags(ProgramOptions programOptions,
                                                       @Nullable String taskId,
                                                       @Nullable MapReduceMetrics.TaskType type,
                                                       @Nullable WorkflowProgramInfo workflowProgramInfo) {
    Map<String, String> tags = Maps.newHashMap();
    if (type != null) {
      tags.put(Constants.Metrics.Tag.MR_TASK_TYPE, type.getId());

      if (taskId != null) {
        String taskMetricsPreference =
          programOptions.getUserArguments().asMap().get(SystemArguments.METRICS_CONTEXT_TASK_INCLUDED);
        boolean taskLevelPreference = taskMetricsPreference == null ? false : Boolean.valueOf(taskMetricsPreference);
        if (taskLevelPreference) {
          tags.put(Constants.Metrics.Tag.INSTANCE_ID, taskId);
        }
      }
    }

    if (workflowProgramInfo != null) {
      workflowProgramInfo.updateMetricsTags(tags);
    }

    return tags;
  }

  //---- following are methods to manage transaction lifecycle for the datasets. This needs to
  //---- be refactored after [TEPHRA-99] and [CDAP-3893] are resolved.

  /**
   * Initializes the transaction-awares.
   */
  private void initializeTransactionAwares() {
    if (transaction == null) {
      return;
    }
    Iterable<TransactionAware> txAwares = Iterables.concat(getDatasetCache().getStaticTransactionAwares(),
                                                           getDatasetCache().getExtraTransactionAwares());
    for (TransactionAware txAware : txAwares) {
      this.txAwares.add(txAware);
      txAware.startTx(transaction);
    }
  }

  @Override
  public <T extends Dataset> T getDataset(String name, Map<String, String> arguments,
                                          AccessType accessType) throws DatasetInstantiationException {
    T dataset = super.getDataset(name, adjustRuntimeArguments(arguments), accessType);
    startDatasetTransaction(dataset);
    return dataset;
  }

  @Override
  public <T extends Dataset> T getDataset(String namespace, String name,
                                          Map<String, String> arguments,
                                          AccessType accessType) throws DatasetInstantiationException {
    T dataset = super.getDataset(namespace, name, adjustRuntimeArguments(arguments), accessType);
    startDatasetTransaction(dataset);
    return dataset;
  }

  /**
   * In MapReduce tasks, table datasets must have the runtime argument HBaseTable.SAFE_INCREMENTS as true.
   */
  private Map<String, String> adjustRuntimeArguments(Map<String, String> args) {
    if (args.containsKey(HBaseTable.SAFE_INCREMENTS)) {
      return args;
    }
    return ImmutableMap.<String, String>builder()
      .putAll(args).put(HBaseTable.SAFE_INCREMENTS, String.valueOf(true)).build();
  }

  /**
   * If a dataset is a new transaction-aware, it starts the transaction and remembers the dataset.
   */
  private <T extends Dataset> void startDatasetTransaction(T dataset) {
    if (dataset instanceof TransactionAware) {
      TransactionAware txAware = (TransactionAware) dataset;

      if (transaction == null) {
        throw new IllegalStateException(
          "Transaction is not supported in the current runtime. Cannot set transaction on dataset "
            + txAware.getTransactionAwareName());
      }

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
   * Calls postTxCommit on all the transaction-aware datasets participating in this context.
   * If any Throwable is encountered, it is suppressed and logged.
   */
  public void postTxCommit() throws Exception {
    if (transaction == null) {
      return;
    }

    for (TransactionAware txAware : txAwares) {
      try {
        txAware.postTxCommit();
      } catch (Throwable t) {
        LOG.error("Unable to perform post-commit in transaction-aware '{}' for transaction {}.",
                  txAware.getTransactionAwareName(), transaction.getTransactionId(), t);
      }
    }
  }

  /**
   * Return {@link AccessEnforcer} to enforce authorization checks in MR tasks.
   */
  public AccessEnforcer getAccessEnforcer() {
    return accessEnforcer;
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
