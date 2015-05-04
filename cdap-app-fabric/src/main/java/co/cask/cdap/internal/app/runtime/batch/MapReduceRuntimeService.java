/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.BatchWritable;
import co.cask.cdap.api.data.batch.DatasetOutputCommitter;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.mapreduce.MapReduce;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.stream.StreamEventDecoder;
import co.cask.cdap.api.templates.plugins.PluginInfo;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.WeakReferenceDelegatorClassLoader;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.utils.ApplicationBundler;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data.stream.StreamInputFormat;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.internal.app.runtime.batch.dataset.DataSetInputFormat;
import co.cask.cdap.internal.app.runtime.batch.dataset.DataSetOutputFormat;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.templates.AdapterDefinition;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.ProvisionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Performs the actual execution of mapreduce job.
 *
 * Service start -> Performs job setup, beforeSubmit and submit job
 * Service run -> Poll for job completion
 * Service triggerStop -> kill job
 * Service stop -> Commit/abort transaction, onFinish, cleanup
 */
final class MapReduceRuntimeService extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceRuntimeService.class);

  /**
   * Do not remove: we need this variable for loading MRClientSecurityInfo class required for communicating with
   * AM in secure mode.
   */
  @SuppressWarnings("unused")
  private org.apache.hadoop.mapreduce.v2.app.MRClientSecurityInfo mrClientSecurityInfo;

  // Name of configuration source if it is set programmatically. This constant is not defined in Hadoop
  private static final String PROGRAMMATIC_SOURCE = "programmatically";

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final MapReduce mapReduce;
  private final MapReduceSpecification specification;
  private final Location programJarLocation;
  private final DynamicMapReduceContext context;
  private final LocationFactory locationFactory;
  private final StreamAdmin streamAdmin;
  private final TransactionSystemClient txClient;
  private final UsageRegistry usageRegistry;

  private Job job;
  private Transaction transaction;
  private Runnable cleanupTask;
  private ClassLoader classLoader;
  private volatile boolean stopRequested;

  MapReduceRuntimeService(CConfiguration cConf, Configuration hConf,
                          MapReduce mapReduce, MapReduceSpecification specification,
                          DynamicMapReduceContext context,
                          Location programJarLocation, LocationFactory locationFactory,
                          StreamAdmin streamAdmin, TransactionSystemClient txClient,
                          UsageRegistry usageRegistry) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.mapReduce = mapReduce;
    this.specification = specification;
    this.programJarLocation = programJarLocation;
    this.locationFactory = locationFactory;
    this.streamAdmin = streamAdmin;
    this.txClient = txClient;
    this.context = context;
    this.usageRegistry = usageRegistry;
  }

  @Override
  protected String getServiceName() {
    return "MapReduceRunner-" + specification.getName();
  }

  @Override
  protected void startUp() throws Exception {
    final Job job = Job.getInstance(new Configuration(hConf));
    Configuration mapredConf = job.getConfiguration();

    if (UserGroupInformation.isSecurityEnabled()) {
      // If runs in secure cluster, this program runner is running in a yarn container, hence not able
      // to get authenticated with the history.
      mapredConf.unset("mapreduce.jobhistory.address");
      mapredConf.setBoolean(Job.JOB_AM_ACCESS_DISABLED, false);

      Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
      LOG.info("Running in secure mode; adding all user credentials: {}", credentials.getAllTokens());
      job.getCredentials().addAll(credentials);
    }

    // We need to hold a strong reference to the ClassLoader until the end of the MapReduce job.
    classLoader = new MapReduceClassLoader(context.getProgram().getClassLoader());
    job.getConfiguration().setClassLoader(new WeakReferenceDelegatorClassLoader(classLoader));
    ClassLoaders.setContextClassLoader(job.getConfiguration().getClassLoader());

    context.setJob(job);

    // both beforeSubmit() and setInput/OutputDataset() may call dataset methods. They must be run inside a tx
    runUserCodeInTx(new TransactionExecutor.Subroutine() {
      // Call the user MapReduce for initialization
      @Override
      public void apply() throws Exception {
        beforeSubmit();

        // set input/output datasets info
        setInputDatasetIfNeeded(job);
        setOutputDatasetIfNeeded(job);
      }
    }, "startUp()");

    // Override user-defined job name, since we set it and depend on the name.
    // https://issues.cask.co/browse/CDAP-2441
    String jobName = job.getJobName();
    if (!jobName.isEmpty()) {
      LOG.warn("Job name {} is being overridden.", jobName);
    }
    job.setJobName(getJobName(context));

    // After calling beforeSubmit, we know what plugins are needed for adapter, hence construct the proper
    // ClassLoader from here and use it for setting up the job
    Location pluginArchive = createPluginArchive(context.getAdapterSpecification(), context.getProgram().getId());
    try {
      if (pluginArchive != null) {
        job.addCacheArchive(pluginArchive.toURI());
      }

      // Alter the configuration ClassLoader to a MapReduceClassLoader that supports plugin
      // It is mainly for standalone mode to have the same ClassLoader as in distributed mode
      // It can only be constructed here because we need to have all adapter plugins information
      classLoader = new MapReduceClassLoader(context.getProgram().getClassLoader(), context.getAdapterSpecification(),
                                             context.getPluginInstantiator());
      job.getConfiguration().setClassLoader(new WeakReferenceDelegatorClassLoader(classLoader));
      ClassLoaders.setContextClassLoader(job.getConfiguration().getClassLoader());

      setOutputClassesIfNeeded(job);
      setMapOutputClassesIfNeeded(job);

      // Prefer our job jar in the classpath
      // Set both old and new keys
      mapredConf.setBoolean("mapreduce.user.classpath.first", true);
      mapredConf.setBoolean(Job.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
      mapredConf.setBoolean(Job.MAPREDUCE_JOB_CLASSLOADER, true);

      // Make CDAP classes (which is in the job.jar created below) to have higher precedence
      // It is needed to override the ApplicationClassLoader to use our implementation
      String yarnAppClassPath = mapredConf.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH, Joiner.on(',')
                                                 .join(YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH));
      mapredConf.set(YarnConfiguration.YARN_APPLICATION_CLASSPATH, "job.jar/lib/*,job.jar/classes," + yarnAppClassPath);

      // set resources for the job
      Resources mapperResources = context.getMapperResources();
      Resources reducerResources = context.getReducerResources();

      // this will determine how much memory and virtual cores the yarn container will run with
      if (mapperResources != null) {
        mapredConf.setInt(Job.MAP_MEMORY_MB, mapperResources.getMemoryMB());
        // Also set the Xmx to be smaller than the container memory.
        mapredConf.set(Job.MAP_JAVA_OPTS, "-Xmx" + (int) (mapperResources.getMemoryMB() * 0.8) + "m");
        setVirtualCores(mapredConf, mapperResources.getVirtualCores(), "MAP");
      }
      if (reducerResources != null) {
        mapredConf.setInt(Job.REDUCE_MEMORY_MB, reducerResources.getMemoryMB());
        // Also set the Xmx to be smaller than the container memory.
        mapredConf.set(Job.REDUCE_JAVA_OPTS, "-Xmx" + (int) (reducerResources.getMemoryMB() * 0.8) + "m");
        setVirtualCores(mapredConf, reducerResources.getVirtualCores(), "REDUCE");
      }

      // replace user's Mapper & Reducer's with our wrappers in job config
      MapperWrapper.wrap(job);
      ReducerWrapper.wrap(job);

      // packaging job jar which includes cdap classes with dependencies
      Location jobJar = buildJobJar(context);

      try {
        Location programJarCopy = copyProgramJar();
        try {
          job.setJar(jobJar.toURI().toString());
          // Localize the program jar, but not add it to class path
          // The ApplicationLoader will create ProgramClassLoader from it
          job.addCacheFile(programJarCopy.toURI());


            MapReduceContextConfig contextConfig = new MapReduceContextConfig(job.getConfiguration());
            // We start long-running tx to be used by mapreduce job tasks.
            Transaction tx = txClient.startLong();
            try {
              // We remember tx, so that we can re-use it in mapreduce tasks
              // Make a copy of the conf and rewrite the template plugin directory to be the plugin archive name
              CConfiguration cConfCopy = cConf;
              if (pluginArchive != null) {
                cConfCopy = CConfiguration.copy(cConf);
                cConfCopy.set(Constants.AppFabric.APP_TEMPLATE_DIR, pluginArchive.getName());
              }
              contextConfig.set(context, cConfCopy, tx, programJarCopy.toURI());

              LOG.info("Submitting MapReduce Job: {}", context);
              // submits job and returns immediately. Shouldn't need to set context ClassLoader.
              job.submit();

              this.job = job;
              this.transaction = tx;
              this.cleanupTask = createCleanupTask(jobJar, programJarCopy, pluginArchive);
            } catch (Throwable t) {
              Transactions.invalidateQuietly(txClient, tx);
              throw Throwables.propagate(t);
            }
        } catch (Throwable t) {
          Locations.deleteQuietly(programJarCopy);
          throw Throwables.propagate(t);
        }
      } catch (Throwable t) {
        Locations.deleteQuietly(jobJar);
        throw Throwables.propagate(t);
      }
    } catch (Throwable t) {
      if (pluginArchive != null) {
        Locations.deleteQuietly(pluginArchive);
      }

      LOG.error("Exception when submitting MapReduce Job: {}", context, t);
      throw Throwables.propagate(t);
    }
  }

  @Override
  protected void run() throws Exception {
    MapReduceMetricsWriter metricsWriter = new MapReduceMetricsWriter(job, context);

    // until job is complete report stats
    while (!job.isComplete()) {
      metricsWriter.reportStats();

      // we report to metrics backend every second, so 1 sec is enough here. That's mapreduce job anyways (not
      // short) ;)
      TimeUnit.SECONDS.sleep(1);
    }

    LOG.info("MapReduce Job is complete, status: {}, job: {}", job.isSuccessful(), context);
    // NOTE: we want to report the final stats (they may change since last report and before job completed)
    metricsWriter.reportStats();
    // If we don't sleep, the final stats may not get sent before shutdown.
    TimeUnit.SECONDS.sleep(2L);

    // If the job is not successful, throw exception so that this service will terminate with a failure state
    // Shutdown will still get executed, but the service will notify failure after that.
    // However, if it's the job is requested to stop (via triggerShutdown, meaning it's a user action), don't throw
    if (!stopRequested) {
      Preconditions.checkState(job.isSuccessful(), "MapReduce execution failure: %s", job.getStatus());
    }
  }

  @Override
  protected void shutDown() throws Exception {
    boolean success = job.isSuccessful();

    try {
      if (success) {
        LOG.info("Committing MapReduce Job transaction: {}", context);
        // committing long running tx: no need to commit datasets, as they were committed in external processes
        // also no need to rollback changes if commit fails, as these changes where performed by mapreduce tasks
        // NOTE: can't call afterCommit on datasets in this case: the changes were made by external processes.
        if (!txClient.commit(transaction)) {
          LOG.warn("MapReduce Job transaction failed to commit");
          throw new TransactionFailureException("Failed to commit transaction for MapReduce " + context.toString());
        }
      } else {
        // invalids long running tx. All writes done by MR cannot be undone at this point.
        txClient.invalidate(transaction.getWritePointer());
      }
    } finally {
      // whatever happens we want to call this
      try {
        onFinish(success);
      } finally {
        context.close();
        cleanupTask.run();
      }
    }
  }

  @Override
  protected void triggerShutdown() {
    try {
      stopRequested = true;
      if (job != null && !job.isComplete()) {
        job.killJob();
      }
    } catch (IOException e) {
      LOG.error("Failed to kill MapReduce job {}", context, e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected Executor executor() {
    // Always execute in new daemon thread.
    return new Executor() {
      @Override
      public void execute(@Nonnull final Runnable runnable) {
        final Thread t = new Thread(new Runnable() {

          @Override
          public void run() {
            // note: this sets logging context on the thread level
            LoggingContextAccessor.setLoggingContext(context.getLoggingContext());
            runnable.run();
          }
        });
        t.setDaemon(true);
        t.setName(getServiceName());
        t.start();
      }
    };
  }

  /**
   * Sets mapper/reducer virtual cores into job configuration if the platform supports it.
   *
   * @param conf The job configuration
   * @param vcores Number of virtual cores to use
   * @param type Either {@code MAP} or {@code REDUCE}.
   */
  private void setVirtualCores(Configuration conf, int vcores, String type) {
    // Try to set virtual cores if the platform supports it
    try {
      String fieldName = type + "_CPU_VCORES";
      Field field = Job.class.getField(fieldName);
      conf.setInt(field.get(null).toString(), vcores);
    } catch (Exception e) {
      // OK to ignore
      // Some older version of hadoop-mr-client doesn't has the VCORES field as vcores was not supported in YARN.
    }
  }

  /**
   * Calls the {@link MapReduce#beforeSubmit(co.cask.cdap.api.mapreduce.MapReduceContext)} method.
   */
  private void beforeSubmit() throws Exception {
    ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(context.getProgram().getClassLoader());
    try {
      mapReduce.beforeSubmit(context);
    } finally {
      ClassLoaders.setContextClassLoader(oldClassLoader);
    }
  }

  /**
   * Calls the {@link MapReduce#onFinish(boolean, co.cask.cdap.api.mapreduce.MapReduceContext)} method.
   */
  private void onFinish(final boolean succeeded) throws TransactionFailureException, InterruptedException {
    runUserCodeInTx(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(context.getProgram().getClassLoader());
        try {
          // TODO this should be done in the output committer, to make the M/R fail if addPartition fails
          boolean success = succeeded;
          Dataset outputDataset = context.getOutputDataset();
          if (outputDataset != null && outputDataset instanceof DatasetOutputCommitter) {
            try {
              if (succeeded) {
                ((DatasetOutputCommitter) outputDataset).onSuccess();
              } else {
                ((DatasetOutputCommitter) outputDataset).onFailure();
              }
            } catch (Throwable t) {
              LOG.error(String.format("Error from %s method of output dataset %s.",
                        succeeded ? "onSuccess" : "onFailure", context.getOutputDatasetName()), t);
              success = false;
            }
          }
          mapReduce.onFinish(success, context);
        } finally {
          ClassLoaders.setContextClassLoader(oldClassLoader);
        }
      }
    }, "onFinish()");
  }

  // instead of using Tephra's TransactionExecutor, we must implement the transaction lifecycle
  // ourselves. This is because the user code may add new datasets through the dynamic mr context,
  // which is not supported by the TransactionExecutor. Instead, we reuse the same txContext
  // (which is managed by the mr context) for all transactions.
  private void runUserCodeInTx(TransactionExecutor.Subroutine userCode, String methodName) {
    TransactionContext txContext = context.getTransactionContext();
    try {
      txContext.start();
      userCode.apply();
      txContext.finish();
    } catch (TransactionFailureException e) {
      abortTransaction(e, "Failed to commit after running " + methodName + ". Aborting transaction.", txContext);
    } catch (Throwable t) {
      abortTransaction(t, "Exception occurred running " + methodName + ". Aborting transaction.", txContext);
    }
  }

  private void abortTransaction(Throwable t, String message, TransactionContext context) {
    try {
      LOG.error(message, t);
      context.abort();
      Throwables.propagate(t);
    } catch (TransactionFailureException e) {
      LOG.error("Failed to abort transaction.", e);
      Throwables.propagate(e);
    }
  }

  @SuppressWarnings("unchecked")
  private void setInputDatasetIfNeeded(Job job) throws IOException {
    String inputDatasetName = context.getInputDatasetName();

    // TODO: It's a hack for stream
    if (inputDatasetName != null && inputDatasetName.startsWith(Constants.Stream.URL_PREFIX)) {
      StreamBatchReadable stream = new StreamBatchReadable(URI.create(inputDatasetName));
      configureStreamInput(job, stream);
      return;
    }

    Dataset dataset = context.getInputDataset();
    if (dataset == null) {
      return;
    }

    LOG.debug("Using Dataset {} as input for MapReduce Job", inputDatasetName);
    // We checked on validation phase that it implements BatchReadable or InputFormatProvider
    if (dataset instanceof BatchReadable) {
      BatchReadable inputDataset = (BatchReadable) dataset;
      List<Split> inputSplits = context.getInputDataSelection();
      if (inputSplits == null) {
        inputSplits = inputDataset.getSplits();
      }
      context.setInput(inputDatasetName, inputSplits);
      DataSetInputFormat.setInput(job, inputDatasetName);
      return;
    }

    // must be input format provider
    InputFormatProvider inputDataset = (InputFormatProvider) dataset;
    String inputFormatClassName = inputDataset.getInputFormatClassName();
    if (inputFormatClassName == null) {
      throw new DataSetException("Input dataset '" + inputDatasetName + "' provided null as the input format");
    }
    job.getConfiguration().set(Job.INPUT_FORMAT_CLASS_ATTR, inputFormatClassName);

    Map<String, String> inputConfig = inputDataset.getInputFormatConfiguration();
    if (inputConfig != null) {
      for (Map.Entry<String, String> entry : inputConfig.entrySet()) {
        job.getConfiguration().set(entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * Sets the configurations for Dataset used for output.
   */
  private void setOutputDatasetIfNeeded(Job job) {
    String outputDatasetName = context.getOutputDatasetName();
    Dataset dataset = context.getOutputDataset();
    if (dataset == null) {
      return;
    }

    LOG.debug("Using Dataset {} as output for MapReduce Job", outputDatasetName);
    // We checked on validation phase that it implements BatchWritable or OutputFormatProvider
    if (dataset instanceof BatchWritable) {
      DataSetOutputFormat.setOutput(job, outputDatasetName);
      return;
    }

    // must be output format provider
    OutputFormatProvider outputDataset = (OutputFormatProvider) dataset;
    String outputFormatClassName = outputDataset.getOutputFormatClassName();
    if (outputFormatClassName == null) {
      throw new DataSetException("Output dataset '" + outputDatasetName + "' provided null as the output format");
    }
    job.getConfiguration().set(Job.OUTPUT_FORMAT_CLASS_ATTR, outputFormatClassName);

    Map<String, String> outputConfig = outputDataset.getOutputFormatConfiguration();
    if (outputConfig != null) {
      for (Map.Entry<String, String> entry : outputConfig.entrySet()) {
        job.getConfiguration().set(entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * Configures the MapReduce Job that uses stream as input.
   *
   * @param job The MapReduce job
   * @param stream A {@link StreamBatchReadable} that carries information about the stream being used for input
   * @throws IOException If fails to configure the job
   */
  private void configureStreamInput(Job job, StreamBatchReadable stream) throws IOException {
    Id.Stream streamId = Id.Stream.from(context.getNamespaceId(), stream.getStreamName());
    StreamConfig streamConfig = streamAdmin.getConfig(streamId);
    Location streamPath = StreamUtils.createGenerationLocation(streamConfig.getLocation(),
                                                               StreamUtils.getGeneration(streamConfig));
    StreamInputFormat.setTTL(job, streamConfig.getTTL());
    StreamInputFormat.setStreamPath(job, streamPath.toURI());
    StreamInputFormat.setTimeRange(job, stream.getStartTime(), stream.getEndTime());

    FormatSpecification formatSpecification = stream.getFormatSpecification();
    if (formatSpecification != null) {
      // this will set the decoder to the correct type. so no need to set it.
      // TODO: allow type projection if the mapper type is compatible (CDAP-1149)
      StreamInputFormat.setBodyFormatSpecification(job, formatSpecification);
    } else {
      String decoderType = stream.getDecoderType();
      if (decoderType == null) {
        // If the user don't specify the decoder, detect the type from Mapper/Reducer
        setStreamEventDecoder(job.getConfiguration());
      } else {
        StreamInputFormat.setDecoderClassName(job, decoderType);
      }
    }

    job.setInputFormatClass(StreamInputFormat.class);

    try {
      usageRegistry.register(context.getProgram().getId(), streamId);
    } catch (Exception e) {
      LOG.warn("Failed to register usage {} -> {}", context.getProgram().getId(), streamId, e);
    }

    LOG.info("Using Stream as input from {}", streamPath.toURI());
  }

  /**
   * Detects what {@link StreamEventDecoder} to use based on the job Mapper/Reducer type. It does so by
   * inspecting the Mapper/Reducer type parameters to figure out what the input type is, and pick the appropriate
   * {@link StreamEventDecoder}.
   *
   * @param hConf The job configuration
   * @throws IOException If fails to detect what decoder to use for decoding StreamEvent.
   */
  @VisibleForTesting
  void setStreamEventDecoder(Configuration hConf) throws IOException {
    // Try to set from mapper
    TypeToken<Mapper> mapperType = resolveClass(hConf, MRJobConfig.MAP_CLASS_ATTR, Mapper.class);
    if (mapperType != null) {
      setStreamEventDecoder(hConf, mapperType);
      return;
    }

    // If there is no Mapper, it's a Reducer only job, hence get the decoder type from Reducer class
    TypeToken<Reducer> reducerType = resolveClass(hConf, MRJobConfig.REDUCE_CLASS_ATTR, Reducer.class);
    setStreamEventDecoder(hConf, reducerType);
  }

  /**
   * Optionally sets the {@link StreamEventDecoder}.
   *
   * @throws IOException If the type is an instance of {@link ParameterizedType} and is not able to determine
   * what {@link StreamEventDecoder} class should use.
   *
   * @param <V> type of the super class
   */
  private <V> void setStreamEventDecoder(Configuration hConf, TypeToken<V> type) throws IOException {
    // The super type must be a parametrized type with <IN_KEY, IN_VALUE, OUT_KEY, OUT_VALUE>
    Type valueType = StreamEvent.class;
    if ((type.getType() instanceof ParameterizedType)) {
      // Try to determine the decoder to use from the first input types
      // The first argument must be LongWritable for it to consumer stream event, as it carries the event timestamp
      Type inputValueType = ((ParameterizedType) type.getType()).getActualTypeArguments()[1];

      // If the Mapper/Reducer class is not parameterized (meaning not extends with parameters),
      // then assume StreamEvent as the input value type.
      // We need to check if the TypeVariable is the same as the one in the parent type.
      // This avoid the case where a subclass that has "class InvalidMapper<I, O> extends Mapper<I, O>"
      if (inputValueType instanceof TypeVariable && inputValueType.equals(type.getRawType().getTypeParameters()[1])) {
        inputValueType = StreamEvent.class;
      }
      // Only Class type is support for inferring stream decoder class
      if (!(inputValueType instanceof Class)) {
        throw new IllegalArgumentException("Input value type not supported for stream input: " + type);
      }
      valueType = inputValueType;
    }
    try {
      StreamInputFormat.inferDecoderClass(hConf, valueType);
    } catch (IllegalArgumentException e) {
      throw new IOException("Type not support for consuming StreamEvent from " + type, e);
    }
  }

  private String getJobName(BasicMapReduceContext context) {
    Id.Program programId = context.getProgram().getId();
    // MRJobClient expects the following format (for RunId to be the first component)
    return String.format("%s.%s.%s.%s.%s",
                         context.getRunId().getId(), ProgramType.MAPREDUCE.name().toLowerCase(),
                         programId.getNamespaceId(), programId.getApplicationId(), programId.getId());
  }

  /**
   * Creates a jar that contains everything that are needed for running the MapReduce program by Hadoop.
   *
   * @return a new {@link Location} containing the job jar
   */
  private Location buildJobJar(BasicMapReduceContext context) throws IOException {
    // Excludes libraries that are for sure not needed.
    // Hadoop - Available from the cluster
    // Spark - MR never uses Spark
    ApplicationBundler appBundler = new ApplicationBundler(ImmutableList.of("org.apache.hadoop",
                                                                            "org.apache.spark"),
                                                           ImmutableList.of("org.apache.hadoop.hbase",
                                                                            "org.apache.hadoop.hive"));
    Id.Program programId = context.getProgram().getId();

    Location jobJar =
      locationFactory.create(String.format("%s.%s.%s.%s.%s.jar",
                                           ProgramType.MAPREDUCE.name().toLowerCase(),
                                           programId.getNamespaceId(), programId.getApplicationId(),
                                           programId.getId(), context.getRunId().getId()));

    LOG.debug("Creating Job jar: {}", jobJar.toURI());

    Set<Class<?>> classes = Sets.newHashSet();
    classes.add(MapReduce.class);
    classes.add(MapperWrapper.class);
    classes.add(ReducerWrapper.class);

    Job jobConf = context.getHadoopJob();
    // We only need to trace the Input/OutputFormat class due to MAPREDUCE-5957 so that those classes are included
    // in the job.jar and be available in the MR system classpath before our job classloader (ApplicationClassLoader)
    // take over the classloading.
    if (cConf.getBoolean(Constants.AppFabric.MAPREDUCE_INCLUDE_CUSTOM_CLASSES)) {
      try {
        Class<? extends InputFormat<?, ?>> inputFormatClass = jobConf.getInputFormatClass();
        LOG.info("InputFormat class: {} {}", inputFormatClass, inputFormatClass.getClassLoader());
        classes.add(inputFormatClass);

        // If it is StreamInputFormat, also add the StreamEventCodec class as well.
        if (StreamInputFormat.class.isAssignableFrom(inputFormatClass)) {
          Class<? extends StreamEventDecoder> decoderType =
            StreamInputFormat.getDecoderClass(jobConf.getConfiguration());
          if (decoderType != null) {
            classes.add(decoderType);
          }
        }
      } catch (Throwable t) {
        LOG.info("InputFormat class not found: {}", t.getMessage(), t);
        // Ignore
      }
      try {
        Class<? extends OutputFormat<?, ?>> outputFormatClass = jobConf.getOutputFormatClass();
        LOG.info("OutputFormat class: {} {}", outputFormatClass, outputFormatClass.getClassLoader());
        classes.add(outputFormatClass);
      } catch (Throwable t) {
        LOG.info("OutputFormat class not found: {}", t.getMessage(), t);
        // Ignore
      }
    }
    // End of MAPREDUCE-5957.

    try {
      Class<?> hbaseTableUtilClass = HBaseTableUtilFactory.getHBaseTableUtilClass();
      classes.add(hbaseTableUtilClass);
    } catch (ProvisionException e) {
      LOG.warn("Not including HBaseTableUtil classes in submitted Job Jar since they are not available");
    }

    ClassLoader oldCLassLoader = ClassLoaders.setContextClassLoader(jobConf.getConfiguration().getClassLoader());
    appBundler.createBundle(jobJar, classes);
    ClassLoaders.setContextClassLoader(oldCLassLoader);

    LOG.info("Built MapReduce Job Jar at {}", jobJar.toURI());
    return jobJar;
  }

  /**
   * Returns a resolved {@link TypeToken} of the given super type by reading a class from the job configuration that
   * extends from super type.
   *
   * @param conf the job configuration
   * @param typeAttr The job configuration attribute for getting the user class
   * @param superType Super type of the class to get from the configuration
   * @param <V> Type of the super type
   * @return A resolved {@link TypeToken} or {@code null} if no such class in the job configuration
   */
  @SuppressWarnings("unchecked")
  private <V> TypeToken<V> resolveClass(Configuration conf, String typeAttr, Class<V> superType) {
    Class<? extends V> userClass = conf.getClass(typeAttr, null, superType);
    if (userClass == null) {
      return null;
    }

    return (TypeToken<V>) TypeToken.of(userClass).getSupertype(superType);
  }

  /**
   * Sets the output key and value classes in the job configuration by inspecting the {@link Mapper} and {@link Reducer}
   * if it is not set by the user.
   *
   * @param job the MapReduce job
   */
  private void setOutputClassesIfNeeded(Job job) {
    Configuration conf = job.getConfiguration();

    // Try to get the type from reducer
    TypeToken<?> type = resolveClass(conf, MRJobConfig.REDUCE_CLASS_ATTR, Reducer.class);

    if (type == null) {
      // Map only job
      type = resolveClass(conf, MRJobConfig.MAP_CLASS_ATTR, Mapper.class);
    }

    // If not able to detect type, nothing to set
    if (type == null || !(type.getType() instanceof ParameterizedType)) {
      return;
    }

    Type[] typeArgs = ((ParameterizedType) type.getType()).getActualTypeArguments();

    // Set it only if the user didn't set it in beforeSubmit
    // The key and value type are in the 3rd and 4th type parameters
    if (!isProgrammaticConfig(conf, MRJobConfig.OUTPUT_KEY_CLASS)) {
      Class<?> cls = TypeToken.of(typeArgs[2]).getRawType();
      LOG.debug("Set output key class to {}", cls);
      job.setOutputKeyClass(cls);
    }
    if (!isProgrammaticConfig(conf, MRJobConfig.OUTPUT_VALUE_CLASS)) {
      Class<?> cls = TypeToken.of(typeArgs[3]).getRawType();
      LOG.debug("Set output value class to {}", cls);
      job.setOutputValueClass(cls);
    }
  }

  /**
   * Sets the map output key and value classes in the job configuration by inspecting the {@link Mapper}
   * if it is not set by the user.
   *
   * @param job the MapReduce job
   */
  private void setMapOutputClassesIfNeeded(Job job) {
    Configuration conf = job.getConfiguration();

    int keyIdx = 2;
    int valueIdx = 3;
    TypeToken<?> type = resolveClass(conf, MRJobConfig.MAP_CLASS_ATTR, Mapper.class);

    if (type == null) {
      // Reducer only job. Use the Reducer input types as the key/value classes.
      type = resolveClass(conf, MRJobConfig.REDUCE_CLASS_ATTR, Reducer.class);
      keyIdx = 0;
      valueIdx = 1;
    }

    // If not able to detect type, nothing to set.
    if (type == null || !(type.getType() instanceof ParameterizedType)) {
      return;
    }

    Type[] typeArgs = ((ParameterizedType) type.getType()).getActualTypeArguments();

    // Set it only if the user didn't set it in beforeSubmit
    // The key and value type are in the 3rd and 4th type parameters
    if (!isProgrammaticConfig(conf, MRJobConfig.MAP_OUTPUT_KEY_CLASS)) {
      Class<?> cls = TypeToken.of(typeArgs[keyIdx]).getRawType();
      LOG.debug("Set map output key class to {}", cls);
      job.setMapOutputKeyClass(cls);
    }
    if (!isProgrammaticConfig(conf, MRJobConfig.MAP_OUTPUT_VALUE_CLASS)) {
      Class<?> cls = TypeToken.of(typeArgs[valueIdx]).getRawType();
      LOG.debug("Set map output value class to {}", cls);
      job.setMapOutputValueClass(cls);
    }
  }

  private boolean isProgrammaticConfig(Configuration conf, String name) {
    String[] sources = conf.getPropertySources(name);
    return sources != null && sources.length > 0 && PROGRAMMATIC_SOURCE.equals(sources[sources.length - 1]);
  }

  /**
   * Creates a temp copy of the program jar.
   *
   * @return a new {@link Location} which contains the same content as the program jar
   */
  private Location copyProgramJar() throws IOException {
    Id.Program programId = context.getProgram().getId();
    Location programJarCopy = locationFactory.create(
      String.format("%s.%s.%s.%s.%s.program.jar",
                    ProgramType.MAPREDUCE.name().toLowerCase(),
                    programId.getNamespaceId(), programId.getApplicationId(),
                    programId.getId(), context.getRunId().getId()));

    ByteStreams.copy(Locations.newInputSupplier(programJarLocation), Locations.newOutputSupplier(programJarCopy));
    LOG.info("Copied Program Jar to {}, source: {}", programJarCopy.toURI(), programJarLocation.toURI());
    return programJarCopy;
  }

  /**
   * Creates a JAR file that contains all the plugin jars that are needed by the job. The plugin directory
   * structure is maintained inside the jar so that MR framework can correctly expand and recreate the required
   * structure.
   *
   * @return the {@link Location} for the archive file or {@code null} if there is no plugin files need to be localized
   */
  @Nullable
  private Location createPluginArchive(@Nullable AdapterDefinition adapterSpec,
                                       Id.Program programId) throws IOException {
    if (adapterSpec == null) {
      return null;
    }

    Set<PluginInfo> pluginInfos = adapterSpec.getPluginInfos();
    if (pluginInfos.isEmpty()) {
      return null;
    }

    // Find plugins that are used by this adapter.
    File pluginDir = new File(cConf.get(Constants.AppFabric.APP_TEMPLATE_PLUGIN_DIR));
    File templatePluginDir = new File(pluginDir, adapterSpec.getTemplate());
    File jarFile = File.createTempFile("plugin", ".jar");
    try {
      String entryPrefix = pluginDir.getName() + "/" + adapterSpec.getTemplate();

      try (JarOutputStream output = new JarOutputStream(new FileOutputStream(jarFile))) {
        // Create the directory entries
        output.putNextEntry(new JarEntry(entryPrefix + "/"));
        output.putNextEntry(new JarEntry(entryPrefix + "/lib/"));

        // copy the plugin jars
        for (PluginInfo plugin : pluginInfos) {
          String entryName = String.format("%s/%s", entryPrefix, plugin.getFileName());
          output.putNextEntry(new JarEntry(entryName));
          Files.copy(new File(templatePluginDir, plugin.getFileName()), output);
        }

        // copy the common plugin lib jars
        for (File libJar : DirUtils.listFiles(new File(templatePluginDir, "lib"), "jar")) {
          String entryName = String.format("%s/lib/%s", entryPrefix, libJar.getName());
          output.putNextEntry(new JarEntry(entryName));
          Files.copy(libJar, output);
        }
      }

      // Copy the jar to a location, based on the location factory
      Location location = locationFactory.create(
        String.format("%s.%s.%s.%s.%s.plugins.jar",
                      ProgramType.MAPREDUCE.name().toLowerCase(),
                      programId.getNamespaceId(), programId.getApplicationId(),
                      programId.getId(), context.getRunId().getId()));
      try {
        Files.copy(jarFile, Locations.newOutputSupplier(location));
        return location;
      } catch (IOException e) {
        Locations.deleteQuietly(location);
        throw e;
      }
    } finally {
      jarFile.delete();
    }
  }

  private Runnable createCleanupTask(final Location... locations) {
    return new Runnable() {

      @Override
      public void run() {
        for (Location location : locations) {
          if (location != null) {
            Locations.deleteQuietly(location);
          }
        }
      }
    };
  }
}
