/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.api.TaskLocalizationContext;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.common.Scope;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.BatchWritable;
import co.cask.cdap.api.data.batch.DatasetOutputCommitter;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.api.spark.SparkProgram;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.common.conf.ConfigurationUtil;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.options.UnsupportedOptionTypeException;
import co.cask.cdap.data.stream.StreamInputFormat;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import co.cask.cdap.data2.dataset2.SingleThreadDatasetCache;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.internal.app.runtime.batch.dataset.CloseableBatchWritable;
import co.cask.cdap.internal.app.runtime.batch.dataset.DatasetInputFormatProvider;
import co.cask.cdap.internal.app.runtime.batch.dataset.DatasetOutputFormatProvider;
import co.cask.cdap.internal.app.runtime.batch.dataset.ForwardingSplitReader;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.spark.SparkConf;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link SparkContext} implementation that is used in both {@link SparkProgram#run(SparkContext)} and
 * inside Spark executor.
 */
public class ExecutionSparkContext extends AbstractSparkContext {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutionSparkContext.class);

  // Cache of Dataset instances that are created through this context
  // for closing all datasets when closing this context. This list does not includes datasets
  // created through the readFromDataset and writeToDataset methods.
  // TODO: (CDAP-3983) The datasets should be managed by the dataset cache. That requires TEPHRA-99.
  private final Map<String, Dataset> datasets;
  private final SparkContextConfig contextConfig;
  private final Transaction transaction;
  private final StreamAdmin streamAdmin;
  private final DynamicDatasetCache datasetCache;
  private final Map<String, File> localizedResources;

  private boolean stopped;
  private SparkFacade sparkFacade;

  /**
   * This constructor is to create instance to be used in Spark executor.
   */
  public ExecutionSparkContext(ApplicationSpecification appSpec,
                               SparkSpecification specification, Id.Program programId, RunId runId,
                               ClassLoader programClassLoader, long logicalStartTime,
                               Map<String, String> runtimeArguments,
                               Transaction transaction, DatasetFramework datasetFramework,
                               TransactionSystemClient txClient,
                               DiscoveryServiceClient discoveryServiceClient,
                               MetricsCollectionService metricsCollectionService,
                               Configuration hConf, StreamAdmin streamAdmin,
                               Map<String, File> localizedResources,
                               @Nullable PluginInstantiator pluginInstantiator,
                               @Nullable WorkflowProgramInfo workflowProgramInfo) {
    this(appSpec, specification, programId, runId, programClassLoader, logicalStartTime, runtimeArguments,
         transaction, datasetFramework, txClient, discoveryServiceClient,
         createMetricsContext(metricsCollectionService, programId, runId),
         createLoggingContext(programId, runId), hConf, streamAdmin, localizedResources, pluginInstantiator,
         workflowProgramInfo);
  }

  /**
   * This constructor is to create instance to be used in the Spark driver.
   */
  public ExecutionSparkContext(ApplicationSpecification appSpec,
                               SparkSpecification specification, Id.Program programId, RunId runId,
                               ClassLoader programClassLoader, long logicalStartTime,
                               Map<String, String> runtimeArguments,
                               Transaction transaction, DatasetFramework datasetFramework,
                               TransactionSystemClient txClient,
                               DiscoveryServiceClient discoveryServiceClient,
                               MetricsContext metricsContext, LoggingContext loggingContext,
                               Configuration hConf, StreamAdmin streamAdmin,
                               Map<String, File> localizedResources,
                               @Nullable PluginInstantiator pluginInstantiator,
                               @Nullable WorkflowProgramInfo workflowProgramInfo) {
    super(appSpec, specification, programId, runId, programClassLoader, logicalStartTime,
          runtimeArguments, discoveryServiceClient, metricsContext, loggingContext, datasetFramework,
          pluginInstantiator, workflowProgramInfo);
    this.datasets = new HashMap<>();
    this.contextConfig = new SparkContextConfig(hConf);
    this.transaction = transaction;
    this.streamAdmin = streamAdmin;
    this.datasetCache = new SingleThreadDatasetCache(systemDatasetInstantiator, txClient,
                                                     new NamespaceId(programId.getNamespace().getId()),
                                                     runtimeArguments, getMetricsContext(), null);
    this.localizedResources = localizedResources;
  }

  @Override
  public <T> T readFromDataset(String datasetName, Class<?> kClass, Class<?> vClass, Map<String, String> userDsArgs) {
    Map<String, String> dsArgs = RuntimeArguments.extractScope(Scope.DATASET, datasetName, getRuntimeArguments());
    dsArgs.putAll(userDsArgs);
    Dataset dataset = instantiateDataset(datasetName, dsArgs);

    try {
      InputFormatProvider inputFormatProvider = new DatasetInputFormatProvider(datasetName, dsArgs, dataset, null,
                                                                               SparkBatchReadableInputFormat.class);

      // get the input format and its configuration from the dataset
      String inputFormatName = inputFormatProvider.getInputFormatClassName();
      // load the input format class
      if (inputFormatName == null) {
        throw new DatasetInstantiationException(
          String.format("Dataset '%s' provided null as the input format class name", datasetName));
      }
      try {
        @SuppressWarnings("unchecked")
        Class<? extends InputFormat> inputFormatClass =
          (Class<? extends InputFormat>) SparkClassLoader.findFromContext().loadClass(inputFormatName);

        // Clone the configuration since it's dataset specification and shouldn't affect the global hConf
        Configuration configuration = new Configuration(contextConfig.getConfiguration());
        ConfigurationUtil.setAll(inputFormatProvider.getInputFormatConfiguration(), configuration);
        return getSparkFacade().createRDD(inputFormatClass, kClass, vClass, configuration);
      } catch (ClassNotFoundException e) {
        throw new DatasetInstantiationException(String.format(
          "Cannot load input format class %s provided by dataset '%s'", inputFormatName, datasetName), e);
      } catch (ClassCastException e) {
        throw new DatasetInstantiationException(String.format(
          "Input format class %s provided by dataset '%s' is not an input format", inputFormatName, datasetName), e);
      }
    } finally {
      commitAndClose(datasetName, dataset);
    }
  }

  @Override
  public <T> void writeToDataset(T rdd, String datasetName, Class<?> kClass, Class<?> vClass,
                                 Map<String, String> userDsArgs) {
    Map<String, String> dsArgs = RuntimeArguments.extractScope(Scope.DATASET, datasetName, getRuntimeArguments());
    dsArgs.putAll(userDsArgs);
    Dataset dataset = instantiateDataset(datasetName, dsArgs);

    try {
      OutputFormatProvider outputFormatProvider = new DatasetOutputFormatProvider(datasetName, dsArgs, dataset,
                                                                                  SparkBatchWritableOutputFormat.class);
      // get the output format and its configuration from the dataset
      String outputFormatName = outputFormatProvider.getOutputFormatClassName();
      // load the output format class
      if (outputFormatName == null) {
        throw new DatasetInstantiationException(
          String.format("Dataset '%s' provided null as the output format class name", datasetName));
      }

      try {
        @SuppressWarnings("unchecked")
        Class<? extends OutputFormat> outputFormatClass =
          (Class<? extends OutputFormat>) SparkClassLoader.findFromContext().loadClass(outputFormatName);

        try {
          // Clone the configuration since it's dataset specification and shouldn't affect the global hConf
          Configuration configuration = new Configuration(contextConfig.getConfiguration());
          ConfigurationUtil.setAll(outputFormatProvider.getOutputFormatConfiguration(), configuration);
          getSparkFacade().saveAsDataset(rdd, outputFormatClass, kClass, vClass, configuration);

          if (dataset instanceof DatasetOutputCommitter) {
            ((DatasetOutputCommitter) dataset).onSuccess();
          }
        } catch (Throwable t) {
          // whatever went wrong, give the dataset a chance to handle the failure
          if (dataset instanceof DatasetOutputCommitter) {
            ((DatasetOutputCommitter) dataset).onFailure();
          }
          throw t;
        }
      } catch (ClassNotFoundException e) {
        throw new DatasetInstantiationException(String.format(
          "Cannot load output format class %s provided by dataset '%s'", outputFormatName, datasetName), e);
      } catch (ClassCastException e) {
        throw new DatasetInstantiationException(String.format(
          "Output format class %s provided by dataset '%s' is not an output format", outputFormatName, datasetName), e);
      }
    } finally {
      commitAndClose(datasetName, dataset);
    }
  }

  @Override
  public <T> T readFromStream(StreamBatchReadable stream, Class<?> vClass) {
    try {
      // Clone the configuration since it's dataset specification and shouldn't affect the global hConf
      Configuration configuration = configureStreamInput(new Configuration(contextConfig.getConfiguration()),
                                                         stream, vClass);
      T streamRDD = getSparkFacade().createRDD(StreamInputFormat.class, LongWritable.class, vClass, configuration);

      // Register for stream usage for the Spark program
      Id.Stream streamId = Id.Stream.from(getProgramId().getNamespace(), stream.getStreamName());
      try {
        streamAdmin.register(getOwners(), streamId);
        streamAdmin.addAccess(new Id.Run(getProgramId(), getRunId().getId()), streamId, AccessType.READ);
      } catch (Exception e) {
        LOG.warn("Failed to registry usage of {} -> {}", streamId, getOwners(), e);
      }
      return streamRDD;
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public <T> T getOriginalSparkContext() {
    return getSparkFacade().getContext();
  }

  @Override
  public synchronized <T extends Dataset> T getDataset(String name, Map<String, String> arguments) {

    String key = name + ImmutableSortedMap.copyOf(arguments).toString();

    @SuppressWarnings("unchecked")
    T dataset = (T) datasets.get(key);
    if (dataset == null) {
      dataset = instantiateDataset(name, arguments);
      datasets.put(key, dataset);
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

  @Override
  public synchronized void close() {
    if (stopped) {
      return;
    }
    stopped = true;
    try {
      if (sparkFacade != null) {
        sparkFacade.stop();
      }
    } finally {
      // No need to flush datasets, just close them
      for (Dataset dataset : datasets.values()) {
        Closeables.closeQuietly(dataset);
      }
    }
  }

  /**
   * For all datasets created through {@link #getDataset(String, Map)} so far that implements
   * {@link TransactionAware}, call {@link TransactionAware#commitTx()}.
   */
  public void flushDatasets() throws Exception {
    for (Dataset dataset : datasets.values()) {
      if (dataset instanceof TransactionAware) {
        ((TransactionAware) dataset).commitTx();
      }
    }
  }

  /**
   * Returns true if this context is closed already.
   */
  public synchronized boolean isStopped() {
    return stopped;
  }

  /**
   * Returns a {@link BatchReadable} that is implemented by the given dataset. This method is expected to be
   * called by {@link SparkBatchReadableInputFormat}.
   *
   * @param datasetName name of the dataset
   * @param arguments arguments for the dataset
   * @param <K> key type for the BatchReadable
   * @param <V> value type for the BatchReadable
   * @return the BatchReadable
   * @throws DatasetInstantiationException if cannot find the dataset
   * @throws IllegalArgumentException if the dataset does not implement BatchReadable
   */
  <K, V> BatchReadable<K, V> getBatchReadable(final String datasetName, Map<String, String> arguments) {
    final Dataset dataset = instantiateDataset(datasetName, arguments);
    Preconditions.checkArgument(dataset instanceof BatchReadable, "Dataset %s of type %s does not implements %s",
                                datasetName, dataset.getClass().getName(), BatchReadable.class.getName());

    @SuppressWarnings("unchecked")
    final BatchReadable<K, V> delegate = (BatchReadable<K, V>) dataset;
    return new BatchReadable<K, V>() {
      @Override
      public List<Split> getSplits() {
        try {
          return delegate.getSplits();
        } finally {
          commitAndClose(datasetName, dataset);
        }
      }

      @Override
      public SplitReader<K, V> createSplitReader(Split split) {
        return new ForwardingSplitReader<K, V>(delegate.createSplitReader(split)) {
          @Override
          public void close() {
            try {
              super.close();
            } finally {
              commitAndClose(datasetName, dataset);
            }
          }
        };
      }
    };
  }

  /**
   * Returns a {@link BatchWritable} that is implemented by the given dataset. The {@link BatchWritable} returned
   * is also {@link Closeable}. Caller of this method is responsible for calling {@link Closeable#close()}} of
   * the returned object. This method is expected to be called by {@link SparkBatchWritableOutputFormat}.
   *
   * @param datasetName name of the dataset
   * @param arguments arguments for the dataset
   * @param <K> key type for the BatchWritable
   * @param <V> value type for the BatchWritable
   * @return the CloseableBatchWritable
   * @throws DatasetInstantiationException if cannot find the dataset
   * @throws IllegalArgumentException if the dataset does not implement BatchWritable
   */
  <K, V> CloseableBatchWritable<K, V> getBatchWritable(final String datasetName, Map<String, String> arguments) {
    final Dataset dataset = instantiateDataset(datasetName, arguments);
    Preconditions.checkArgument(dataset instanceof BatchWritable, "Dataset %s of type %s does not implements %s",
                                datasetName, dataset.getClass().getName(), BatchWritable.class.getName());

    @SuppressWarnings("unchecked")
    final BatchWritable<K, V> delegate = (BatchWritable<K, V>) dataset;
    return new CloseableBatchWritable<K, V>() {
      @Override
      public void write(K key, V value) {
        delegate.write(key, value);
      }

      @Override
      public void close() throws IOException {
        commitAndClose(datasetName, dataset);
      }
    };
  }

  @Override
  public SparkConf getSparkConf() {
    // This is an internal method.
    // Throwing exception here to avoid being called inside CDAP wrongly, since we shouldn't call this during execution.
    throw new UnsupportedOperationException("getSparkConf shouldn't be called in execution context");
  }

  public SparkContextConfig getContextConfig() {
    return contextConfig;
  }

  /**
   * Sets the {@link SparkFacade} for this context.
   */
  synchronized void setSparkFacade(SparkFacade sparkFacade) {
    this.sparkFacade = sparkFacade;
    if (stopped) {
      sparkFacade.stop();
    }
  }

  /**
   * Returns the transaction for the Spark program execution.
   */
  public Transaction getTransaction() {
    return transaction;
  }

  /**
   * Returns the {@link SparkFacade}.
   *
   * @throws IllegalStateException if the {@link SparkFacade} is not yet set.
   */
  private synchronized SparkFacade getSparkFacade() {
    Preconditions.checkState(sparkFacade != null,
                             "Spark Context not available in the current execution context");
    return sparkFacade;
  }

  /**
   * Calls {@link TransactionAware#commitTx()} on the given dataset followed by a {@link Closeable#close()} call.
   */
  private void commitAndClose(String name, Dataset dataset) {
    try {
      if (dataset instanceof TransactionAware) {
        try {
          ((TransactionAware) dataset).commitTx();
        } catch (Exception e) {
          // Nothing much we can do besides propagating it.
          // Since the propagation will end up in the Spark Framework, we do the logging here.
          LOG.error("Failed to commit dataset changes for dataset {} of type {}", name, dataset.getClass(), e);
          throw Throwables.propagate(e);
        }
      }
    } finally {
      Closeables.closeQuietly(dataset);
    }
  }

  /**
   * Sets up the configuration for reading from stream through {@link StreamInputFormat}.
   */
  private Configuration configureStreamInput(Configuration configuration,
                                             StreamBatchReadable stream, Class<?> vClass) throws IOException {

    Id.Stream streamId = Id.Stream.from(getProgramId().getNamespace(), stream.getStreamName());

    StreamConfig streamConfig = streamAdmin.getConfig(streamId);
    Location streamPath = StreamUtils.createGenerationLocation(streamConfig.getLocation(),
                                                               StreamUtils.getGeneration(streamConfig));
    StreamInputFormat.setTTL(configuration, streamConfig.getTTL());
    StreamInputFormat.setStreamPath(configuration, streamPath.toURI());
    StreamInputFormat.setTimeRange(configuration, stream.getStartTime(), stream.getEndTime());

    String decoderType = stream.getDecoderType();
    if (decoderType == null) {
      // If the user don't specify the decoder, detect the type
      StreamInputFormat.inferDecoderClass(configuration, vClass);
    } else {
      StreamInputFormat.setDecoderClassName(configuration, decoderType);
    }
    return configuration;
  }

  /**
   * Creates a new instance of dataset.
   */
  private <T extends Dataset> T instantiateDataset(String datasetName, Map<String, String> arguments) {
    // bypass = true, so that the dataset is not added to the factory's cache etc.
    T dataset = datasetCache.getDataset(datasetName, arguments, true);

    // Provide the current long running transaction to the given dataset.
    if (dataset instanceof TransactionAware) {
      ((TransactionAware) dataset).startTx(transaction);
    }
    return dataset;
  }

  @Override
  public TaskLocalizationContext getTaskLocalizationContext() {
    return new SparkLocalizationContext(localizedResources);
  }

  @Override
  public void localize(String name, URI uri) {
    throw new UnsupportedOptionTypeException("Resources cannot be localized in Spark closures.");
  }

  @Override
  public void localize(String name, URI uri, boolean archive) {
    throw new UnsupportedOptionTypeException("Resources cannot be localized in Spark closures.");
  }
}
