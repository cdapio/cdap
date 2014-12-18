/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.mapreduce.MapReduce;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.stream.StreamEventDecoder;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.CombineClassLoader;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.data.stream.StreamInputFormat;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.internal.app.runtime.batch.dataset.DataSetInputFormat;
import co.cask.cdap.internal.app.runtime.batch.dataset.DataSetOutputFormat;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.tephra.DefaultTransactionExecutor;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.ProvisionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.ApplicationBundler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

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

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final MapReduce mapReduce;
  private final MapReduceSpecification specification;
  private final Location programJarLocation;
  private final BasicMapReduceContext context;
  private final LocationFactory locationFactory;
  private final StreamAdmin streamAdmin;
  private final TransactionSystemClient txClient;
  private Job job;
  private Transaction transaction;
  private Runnable cleanupTask;
  private volatile boolean stopRequested;

  MapReduceRuntimeService(CConfiguration cConf, Configuration hConf,
                          MapReduce mapReduce, MapReduceSpecification specification, BasicMapReduceContext context,
                          Location programJarLocation, LocationFactory locationFactory,
                          StreamAdmin streamAdmin, TransactionSystemClient txClient) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.mapReduce = mapReduce;
    this.specification = specification;
    this.programJarLocation = programJarLocation;
    this.context = context;
    this.locationFactory = locationFactory;
    this.streamAdmin = streamAdmin;
    this.txClient = txClient;
  }

  @Override
  protected String getServiceName() {
    return "MapReduceRunner-" + specification.getName();
  }

  @Override
  protected void startUp() throws Exception {
    Job job = Job.getInstance(new Configuration(hConf));
    Configuration mapredConf = job.getConfiguration();

    Resources mapperResources = specification.getMapperResources();
    Resources reducerResources = specification.getReducerResources();

    // this will determine how much memory and vcores the yarn container will run with
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

    // Prefer our job jar in the classpath
    // Set both old and new keys
    mapredConf.setBoolean("mapreduce.user.classpath.first", true);
    mapredConf.setBoolean(Job.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

    if (UserGroupInformation.isSecurityEnabled()) {
      // If runs in secure cluster, this program runner is running in a yarn container, hence not able
      // to get authenticated with the history and MR-AM.
      mapredConf.unset("mapreduce.jobhistory.address");
      mapredConf.setBoolean(MRJobConfig.JOB_AM_ACCESS_DISABLED, true);

      Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
      LOG.info("Running in secure mode; adding all user credentials: {}", credentials.getAllTokens());
      job.getCredentials().addAll(credentials);
    }

    // Create a classloader that have the context/system classloader as parent and the program classloader as child
    ClassLoader classLoader = new CombineClassLoader(
      Objects.firstNonNull(Thread.currentThread().getContextClassLoader(), ClassLoader.getSystemClassLoader()),
      ImmutableList.of(context.getProgram().getClassLoader())
    );

    job.getConfiguration().setClassLoader(classLoader);
    context.setJob(job);

    // Call the user MapReduce for initialization
    beforeSubmit();

    // set input/output datasets info
    setInputDatasetIfNeeded(job);
    setOutputDatasetIfNeeded(job);

    // replace user's Mapper & Reducer's with our wrappers in job config
    MapperWrapper.wrap(job);
    ReducerWrapper.wrap(job);

    // packaging job jar which includes cdap classes with dependencies
    // NOTE: user's jar is added to classpath separately to leave the flexibility in future to create and use separate
    //       classloader when executing user code. We need to submit a copy of the program jar because
    //       in distributed mode this returns program path on HDFS, not localized, which may cause race conditions
    //       if we allow deploying new program while existing is running. To prevent races we submit a temp copy

    Location jobJar = buildJobJar(context);
    try {
      try {
        Location programJarCopy = copyProgramJar();
        try {
          job.setJar(jobJar.toURI().toString());
          job.addFileToClassPath(new Path(programJarCopy.toURI()));

          MapReduceContextConfig contextConfig = new MapReduceContextConfig(job);
          // We start long-running tx to be used by mapreduce job tasks.
          Transaction tx = txClient.startLong();
          try {
            // We remember tx, so that we can re-use it in mapreduce tasks
            contextConfig.set(context, cConf, tx, programJarCopy.getName());

            LOG.info("Submitting MapReduce Job: {}", context);
            ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(job.getConfiguration().getClassLoader());
            try {
              // submits job and returns immediately
              job.submit();
            } finally {
              Thread.currentThread().setContextClassLoader(oldClassLoader);
            }

            this.job = job;
            this.transaction = tx;
            this.cleanupTask = createCleanupTask(jobJar, programJarCopy);
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
      public void execute(final Runnable runnable) {
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
  private void beforeSubmit() throws TransactionFailureException, InterruptedException {
    createTransactionExecutor().execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(mapReduce.getClass().getClassLoader());
        try {
          mapReduce.beforeSubmit(context);
        } finally {
          Thread.currentThread().setContextClassLoader(oldClassLoader);
        }
      }
    });
  }

  /**
   * Calls the {@link MapReduce#onFinish(boolean, co.cask.cdap.api.mapreduce.MapReduceContext)} method.
   */
  private void onFinish(final boolean succeeded) throws TransactionFailureException, InterruptedException {
    createTransactionExecutor().execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        mapReduce.onFinish(succeeded, context);
      }
    });
  }

  /**
   * Creates a {@link TransactionExecutor} with all the {@link co.cask.tephra.TransactionAware} in the context.
   */
  private TransactionExecutor createTransactionExecutor() {
    return new DefaultTransactionExecutor(txClient, context.getDatasetInstantiator().getTransactionAware());
  }

  @SuppressWarnings("unchecked")
  private void setInputDatasetIfNeeded(Job job) throws IOException {
    String inputDatasetName = context.getInputDatasetName();
    if (inputDatasetName == null) {
      // trying to init input dataset from spec
      inputDatasetName = context.getSpecification().getInputDataSet();
    }
    if (inputDatasetName == null) {
      // not using a dataset as input
      return;
    }

    // TODO: It's a hack for stream
    if (inputDatasetName.startsWith("stream://")) {
      StreamBatchReadable stream = new StreamBatchReadable(URI.create(inputDatasetName));
      configureStreamInput(job, stream);
      return;
    }

    LOG.debug("Using Dataset {} as input for MapReduce Job", inputDatasetName);

    // We checked on validation phase that it implements BatchReadable or InputFormatProvider
    Dataset dataset = context.getDataset(inputDatasetName);
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
    Class<? extends InputFormat> inputFormatClass = inputDataset.getInputFormatClass();
    if (inputFormatClass == null) {
      throw new DataSetException("Input dataset '" + inputDatasetName + "' provided null as the input format");
    }
    job.setInputFormatClass(inputFormatClass);
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
    // whatever was set into mapReduceContext e.g. during beforeSubmit(..) takes precedence
    if (outputDatasetName == null) {
      // trying to init output dataset from spec
      outputDatasetName = context.getSpecification().getOutputDataSet();
    }
    if (outputDatasetName == null) {
      // not using a dataset as input
      return;
    }

    LOG.debug("Using Dataset {} as output for MapReduce Job", outputDatasetName);

    // We checked on validation phase that it implements BatchWritable or OutputFormatProvider
    Dataset dataset = context.getDataset(outputDatasetName);
    if (dataset instanceof BatchWritable) {
      DataSetOutputFormat.setOutput(job, outputDatasetName);
      return;
    }

    // must be output format provider
    OutputFormatProvider outputDataset = (OutputFormatProvider) dataset;
    Class<? extends OutputFormat> outputFormatClass = outputDataset.getOutputFormatClass();
    if (outputFormatClass == null) {
      throw new DataSetException("Output dataset '" + outputDatasetName + "' provided null as the output format");
    }
    job.setOutputFormatClass(outputFormatClass);
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
    StreamConfig streamConfig = streamAdmin.getConfig(stream.getStreamName());
    Location streamPath = StreamUtils.createGenerationLocation(streamConfig.getLocation(),
                                                               StreamUtils.getGeneration(streamConfig));
    StreamInputFormat.setTTL(job, streamConfig.getTTL());
    StreamInputFormat.setStreamPath(job, streamPath.toURI());
    StreamInputFormat.setTimeRange(job, stream.getStartTime(), stream.getEndTime());

    String decoderType = stream.getDecoderType();
    if (decoderType == null) {
      // If the user don't specify the decoder, detect the type from Mapper/Reducer
      setStreamEventDecoder(job);
    } else {
      StreamInputFormat.setDecoderClassName(job, decoderType);
    }
    job.setInputFormatClass(StreamInputFormat.class);

    LOG.info("Using Stream as input from {}", streamPath.toURI());
  }

  /**
   * Detects what {@link StreamEventDecoder} to use based on the job Mapper/Reducer type. It does so by
   * inspecting the Mapper/Reducer type parameters to figure out what the input type is, and pick the appropriate
   * {@link StreamEventDecoder}.
   *
   * @param job The MapReduce job
   * @throws IOException If fails to detect what decoder to use for decoding StreamEvent.
   */
  private void setStreamEventDecoder(Job job) throws IOException {
    // Try to set from mapper
    if (setStreamEventDecoder(job, MRJobConfig.MAP_CLASS_ATTR, Mapper.class)) {
      return;
    }
    // If there is no Mapper, it's a Reducer only job, hence get the decoder type from Reducer class
    if (!setStreamEventDecoder(job, MRJobConfig.REDUCE_CLASS_ATTR, Reducer.class)) {
      throw new IOException("Failed to consume StreamEvent without Mapper/Reducer");
    }
  }

  /**
   * Optionally sets the {@link StreamEventDecoder}.
   *
   * @param job The MapReduce job
   * @param typeAttr The job configuration attribute for getting the user class
   * @param matchSuperType Super type of the class to get from the configuration
   * @return {@code true} if a decoder is set, {@code false} if no user class is set in the configuration with the given
   *         attribute name
   * @throws IOException If the user class is found in the configuration, but not able to determine what
   *         {@link StreamEventDecoder} class should use.
   */
  private boolean setStreamEventDecoder(Job job, String typeAttr, Class<?> matchSuperType) throws IOException {
    Class<?> userClass = job.getConfiguration().getClass(typeAttr, null, matchSuperType);
    if (userClass == null) {
      return false;
    }

    TypeToken<?>.TypeSet superTypes = TypeToken.of(userClass).getTypes();

    for (TypeToken<?> superType : Iterables.concat(superTypes.classes(), superTypes.interfaces())) {
      if (!matchSuperType.equals(superType.getRawType())) {
        continue;
      }
      // The super type must be a parameterized type with <IN_KEY, IN_VALUE, OUT_KEY, OUT_VALUE>
      if (!ParameterizedType.class.isAssignableFrom(superType.getType().getClass())) {
        continue;
      }

      // Try to determine the decoder to use from the first input types
      // The first argument must be LongWritable for it to consumer stream event, as it carries the event timestamp
      Type[] typeArgs = ((ParameterizedType) superType.getType()).getActualTypeArguments();
      if (typeArgs.length < 2 || !LongWritable.class.equals(typeArgs[0])) {
        continue;
      }
      try {
        StreamInputFormat.inferDecoderClass(job.getConfiguration(), typeArgs[1]);
        return true;
      } catch (IllegalArgumentException iae) {
        LOG.debug("Failed to set decoder", iae);
      }
    }
    throw new IOException("Failed to determine decoder for consuming StreamEvent from " + userClass);
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
                                           programId.getAccountId(), programId.getApplicationId(),
                                           programId.getId(), context.getRunId().getId()));

    LOG.debug("Creating Job jar: {}", jobJar.toURI());

    Set<Class<?>> classes = Sets.newHashSet();
    classes.add(MapReduce.class);
    classes.add(MapperWrapper.class);
    classes.add(ReducerWrapper.class);

    Job jobConf = context.getHadoopJob();
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
    try {
      Class<?> hbaseTableUtilClass = new HBaseTableUtilFactory().get().getClass();
      classes.add(hbaseTableUtilClass);
    } catch (ProvisionException e) {
      LOG.warn("Not including HBaseTableUtil classes in submitted Job Jar since they are not available");
    }

    ClassLoader oldCLassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(jobConf.getConfiguration().getClassLoader());
    appBundler.createBundle(jobJar, classes);
    Thread.currentThread().setContextClassLoader(oldCLassLoader);

    LOG.info("Built MapReduce Job Jar at {}", jobJar.toURI());
    return jobJar;
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
                    programId.getAccountId(), programId.getApplicationId(),
                    programId.getId(), context.getRunId().getId()));

    ByteStreams.copy(Locations.newInputSupplier(programJarLocation), Locations.newOutputSupplier(programJarCopy));
    LOG.info("Copied Program Jar to {}, source: {}", programJarCopy.toURI(), programJarLocation.toURI());
    return programJarCopy;
  }

  private Runnable createCleanupTask(final Location... locations) {
    return new Runnable() {

      @Override
      public void run() {
        for (Location location : locations) {
          Locations.deleteQuietly(location);
        }
      }
    };
  }
}
