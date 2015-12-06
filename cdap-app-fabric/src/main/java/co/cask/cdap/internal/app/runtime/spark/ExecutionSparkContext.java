/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.api.stream.StreamEventDecoder;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.data.dataset.DatasetInstantiator;
import co.cask.cdap.data.stream.StreamInputFormat;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.internal.app.runtime.spark.dataset.CloseableBatchWritable;
import co.cask.cdap.internal.app.runtime.spark.dataset.SparkDatasetInputFormat;
import co.cask.cdap.internal.app.runtime.spark.dataset.SparkDatasetOutputFormat;
import co.cask.cdap.proto.Id;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
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
import java.io.IOException;
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
  private final Map<String, Dataset> datasets;
  private final Configuration hConf;
  private final Transaction transaction;
  private final StreamAdmin streamAdmin;
  private final DatasetInstantiator datasetInstantiator;
  private boolean stopped;
  private SparkFacade sparkFacade;

  public ExecutionSparkContext(ApplicationSpecification appSpec,
                               SparkSpecification specification, Id.Program programId, RunId runId,
                               ClassLoader programClassLoader, long logicalStartTime,
                               Map<String, String> runtimeArguments,
                               Transaction transaction, DatasetFramework datasetFramework,
                               DiscoveryServiceClient discoveryServiceClient,
                               MetricsCollectionService metricsCollectionService,
                               Configuration hConf, StreamAdmin streamAdmin, @Nullable WorkflowToken workflowToken) {
    this(appSpec, specification, programId, runId, programClassLoader, logicalStartTime, runtimeArguments,
         transaction, datasetFramework, discoveryServiceClient,
         createMetricsContext(metricsCollectionService, programId, runId),
         createLoggingContext(programId, runId), hConf, streamAdmin, workflowToken);
  }

  public ExecutionSparkContext(ApplicationSpecification appSpec,
                               SparkSpecification specification, Id.Program programId, RunId runId,
                               ClassLoader programClassLoader, long logicalStartTime,
                               Map<String, String> runtimeArguments,
                               Transaction transaction, DatasetFramework datasetFramework,
                               DiscoveryServiceClient discoveryServiceClient, MetricsContext metricsContext,
                               LoggingContext loggingContext, Configuration hConf, StreamAdmin streamAdmin,
                               WorkflowToken workflowToken) {
    super(appSpec, specification, programId, runId, programClassLoader, logicalStartTime,
          runtimeArguments, discoveryServiceClient, metricsContext, loggingContext, workflowToken);

    this.datasets = new HashMap<>();
    this.hConf = hConf;
    this.transaction = transaction;
    this.streamAdmin = streamAdmin;
    this.datasetInstantiator = new DatasetInstantiator(programId.getNamespace(), datasetFramework,
                                                       programClassLoader, getOwners(), getMetricsContext());
  }

  @Override
  public <T> T readFromDataset(String datasetName, Class<?> kClass, Class<?> vClass, Map<String, String> userDsArgs) {
    // Clone the configuration since it's dataset specification and shouldn't affect the global hConf
    Configuration configuration = new Configuration(hConf);

    // first try if it is InputFormatProvider
    Map<String, String> dsArgs = RuntimeArguments.extractScope(Scope.DATASET, datasetName, getRuntimeArguments());
    dsArgs.putAll(userDsArgs);
    Dataset dataset = instantiateDataset(datasetName, dsArgs);
    try {
      if (dataset instanceof InputFormatProvider) {
        // get the input format and its configuration from the dataset
        String inputFormatName = ((InputFormatProvider) dataset).getInputFormatClassName();
        // load the input format class
        if (inputFormatName == null) {
          throw new DatasetInstantiationException(
            String.format("Dataset '%s' provided null as the input format class name", datasetName));
        }
        Class<? extends InputFormat> inputFormatClass;
        try {
          @SuppressWarnings("unchecked")
          Class<? extends InputFormat> ifClass =
            (Class<? extends InputFormat>) SparkClassLoader.findFromContext().loadClass(inputFormatName);
          inputFormatClass = ifClass;
          Map<String, String> inputConfig = ((InputFormatProvider) dataset).getInputFormatConfiguration();
          if (inputConfig != null) {
            for (Map.Entry<String, String> entry : inputConfig.entrySet()) {
              configuration.set(entry.getKey(), entry.getValue());
            }
          }
        } catch (ClassNotFoundException e) {
          throw new DatasetInstantiationException(String.format(
            "Cannot load input format class %s provided by dataset '%s'", inputFormatName, datasetName), e);
        } catch (ClassCastException e) {
          throw new DatasetInstantiationException(String.format(
            "Input format class %s provided by dataset '%s' is not an input format", inputFormatName, datasetName), e);
        }
        return getSparkFacade().createRDD(inputFormatClass, kClass, vClass, configuration);
      }
    } finally {
      commitAndClose(datasetName, dataset);
    }

    // it must be supported by SparkDatasetInputFormat
    SparkDatasetInputFormat.setDataset(configuration, datasetName, dsArgs);
    return getSparkFacade().createRDD(SparkDatasetInputFormat.class, kClass, vClass, configuration);
  }

  @Override
  public <T> void writeToDataset(T rdd, String datasetName, Class<?> kClass, Class<?> vClass,
                                 Map<String, String> userDsArgs) {
    // Clone the configuration since it's dataset specification and shouldn't affect the global hConf
    Configuration configuration = new Configuration(hConf);

    // first try if it is OutputFormatProvider
    Map<String, String> dsArgs = RuntimeArguments.extractScope(Scope.DATASET, datasetName, getRuntimeArguments());
    dsArgs.putAll(userDsArgs);
    Dataset dataset = instantiateDataset(datasetName, dsArgs);
    try {
      if (dataset instanceof OutputFormatProvider) {
        // get the output format and its configuration from the dataset
        String outputFormatName = ((OutputFormatProvider) dataset).getOutputFormatClassName();
        // load the output format class
        if (outputFormatName == null) {
          throw new DatasetInstantiationException(
            String.format("Dataset '%s' provided null as the output format class name", datasetName));
        }
        Class<? extends OutputFormat> outputFormatClass;
        try {
          @SuppressWarnings("unchecked")
          Class<? extends OutputFormat> ofClass =
            (Class<? extends OutputFormat>) SparkClassLoader.findFromContext().loadClass(outputFormatName);
          outputFormatClass = ofClass;
          Map<String, String> outputConfig = ((OutputFormatProvider) dataset).getOutputFormatConfiguration();
          if (outputConfig != null) {
            for (Map.Entry<String, String> entry : outputConfig.entrySet()) {
              configuration.set(entry.getKey(), entry.getValue());
            }
          }
        } catch (ClassNotFoundException e) {
          throw new DatasetInstantiationException(String.format(
            "Cannot load input format class %s provided by dataset '%s'", outputFormatName, datasetName), e);
        } catch (ClassCastException e) {
          throw new DatasetInstantiationException(String.format(
            "Input format class %s provided by dataset '%s' is not an input format", outputFormatName, datasetName), e);
        }
        try {
          getSparkFacade().saveAsDataset(rdd, outputFormatClass, kClass, vClass, configuration);
        } catch (Throwable t) {
          // whatever went wrong, give the dataset a chance to handle the failure
          if (dataset instanceof DatasetOutputCommitter) {
            ((DatasetOutputCommitter) dataset).onFailure();
          }
          throw t;
        }
        if (dataset instanceof DatasetOutputCommitter) {
          ((DatasetOutputCommitter) dataset).onSuccess();
        }
        return;
      }
    } finally {
      commitAndClose(datasetName, dataset);
    }

    // it must be supported by SparkDatasetOutputFormat
    SparkDatasetOutputFormat.setDataset(hConf, datasetName, dsArgs);
    getSparkFacade().saveAsDataset(rdd, SparkDatasetOutputFormat.class, kClass, vClass, new Configuration(hConf));
  }

  @Override
  public <T> T readFromStream(String streamName, Class<?> vClass,
                              long startTime, long endTime, Class<? extends StreamEventDecoder> decoderType) {

    StreamBatchReadable stream = (decoderType == null)
      ? new StreamBatchReadable(streamName, startTime, endTime)
      : new StreamBatchReadable(streamName, startTime, endTime, decoderType);

    try {
      // Clone the configuration since it's dataset specification and shouldn't affect the global hConf
      Configuration configuration = configureStreamInput(new Configuration(hConf), stream, vClass);
      T streamRDD = getSparkFacade().createRDD(StreamInputFormat.class, LongWritable.class, vClass, configuration);

      // Register for stream usage for the Spark program
      Id.Stream streamId = Id.Stream.from(getProgramId().getNamespace(), streamName);
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
    Map<String, String> datasetArgs = RuntimeArguments.extractScope(Scope.DATASET, name, getRuntimeArguments());
    datasetArgs.putAll(arguments);

    String key = name + ImmutableSortedMap.copyOf(datasetArgs).toString();

    @SuppressWarnings("unchecked")
    T dataset = (T) datasets.get(key);
    if (dataset == null) {
      dataset = instantiateDataset(name, datasetArgs);
      datasets.put(key, dataset);
    }
    return dataset;
  }

  @Override
  public synchronized void close() {
    if (stopped) {
      return;
    }
    stopped = true;
    if (sparkFacade != null) {
      sparkFacade.stop();
    }
    // No need to flush datasets, just close them
    for (Dataset dataset : datasets.values()) {
      Closeables.closeQuietly(dataset);
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
   * called by {@link SparkDatasetInputFormat}.
   *
   * @param datasetName name of the dataset
   * @param arguments arguments for the dataset
   * @param <K> key type for the BatchReadable
   * @param <V> value type for the BatchReadable
   * @return the BatchReadable
   * @throws DatasetInstantiationException if cannot find the dataset
   * @throws IllegalArgumentException if the dataset does not implement BatchReadable
   */
  public <K, V> BatchReadable<K, V> getBatchReadable(final String datasetName, Map<String, String> arguments) {
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
   * the returned object. This method is expected to be called by {@link SparkDatasetOutputFormat}.
   *
   * @param datasetName name of the dataset
   * @param arguments arguments for the dataset
   * @param <K> key type for the BatchWritable
   * @param <V> value type for the BatchWritable
   * @return the CloseableBatchWritable
   * @throws DatasetInstantiationException if cannot find the dataset
   * @throws IllegalArgumentException if the dataset does not implement BatchWritable
   */
  public <K, V> CloseableBatchWritable<K, V> getBatchWritable(final String datasetName, Map<String, String> arguments) {
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
    T dataset = datasetInstantiator.getDataset(datasetName, arguments);

    // Provide the current long running transaction to the given dataset.
    // Remove it from the transaction aware list from the dataset instantiator, since we won't use it in this context
    // Need to remove it to avoid memory leak.
    if (dataset instanceof TransactionAware) {
      ((TransactionAware) dataset).startTx(transaction);
      datasetInstantiator.removeTransactionAware((TransactionAware) dataset);
    }
    return dataset;
  }
}
