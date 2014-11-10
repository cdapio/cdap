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

import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.mapreduce.MapReduce;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.CombineClassLoader;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data.stream.TextStreamInputFormat;
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
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.ProvisionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import java.net.URI;
import java.util.List;
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

    int mapperMemory = specification.getMapperMemoryMB();
    int reducerMemory = specification.getReducerMemoryMB();
    // this will determine how much memory the yarn container will run with
    if (mapperMemory > 0) {
      mapredConf.setInt(Job.MAP_MEMORY_MB, mapperMemory);
      // Also set the Xmx to be smaller than the container memory.
      mapredConf.set(Job.MAP_JAVA_OPTS, "-Xmx" + (int) (mapperMemory * 0.8) + "m");
    }
    if (reducerMemory > 0) {
      mapredConf.setInt(Job.REDUCE_MEMORY_MB, reducerMemory);
      // Also set the Xmx to be smaller than the container memory.
      mapredConf.set(Job.REDUCE_JAVA_OPTS, "-Xmx" + (int) (reducerMemory * 0.8) + "m");
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

    // replace user's Mapper & Reducer's with our wrappers in job config
    wrapMapperReducer(job);
    wrapReducerClassIfNeeded(job);

    // set input/output datasets info
    setInputDataSetIfNeeded(job);
    setOutputDataSetIfNeeded(job);

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

  /**
   * Changes the {@link Mapper} class in the Job to our {@link MapperWrapper} if the user job has mapper.
   */
  private void wrapMapperReducer(Job job) throws ClassNotFoundException {
    // NOTE: we don't use job.getMapperClass() as we cannot (and don't want to) access user class here
    String mapClass = job.getConfiguration().get(MRJobConfig.MAP_CLASS_ATTR);
    if (mapClass != null) {
      job.getConfiguration().set(MapperWrapper.ATTR_MAPPER_CLASS, mapClass);
      // yes, it is a subclass of Mapper
      Class<? extends Mapper> wrapperClass = MapperWrapper.class;
      job.setMapperClass(wrapperClass);
    }
  }

  /**
   * Changes the {@link Reducer} class in the Job to our {@link ReducerWrapper} if the user job has reducer.
   */
  private void wrapReducerClassIfNeeded(Job job) throws ClassNotFoundException {
    // NOTE: we don't use job.getReducerClass() as we cannot (and don't want to) access user class here
    String reducerClass = job.getConfiguration().get(MRJobConfig.REDUCE_CLASS_ATTR);
    if (reducerClass != null) {
      job.getConfiguration().set(ReducerWrapper.ATTR_REDUCER_CLASS, reducerClass);
      // yes, it is a subclass of Reducer
      Class<? extends Reducer> wrapperClass = ReducerWrapper.class;
      job.setReducerClass(wrapperClass);
    }
  }

  @SuppressWarnings("unchecked")
  private void setInputDataSetIfNeeded(Job job) throws IOException {
    String inputDataSetName = context.getInputDatasetName();
    if (inputDataSetName == null) {
      // trying to init input dataset from spec
      inputDataSetName = context.getSpecification().getInputDataSet();
    }

    if (inputDataSetName != null) {
      // TODO: It's a hack for stream
      if (inputDataSetName.startsWith("stream://")) {
        StreamBatchReadable stream = new StreamBatchReadable(URI.create(inputDataSetName));
        StreamConfig streamConfig = streamAdmin.getConfig(stream.getStreamName());
        Location streamPath = StreamUtils.createGenerationLocation(streamConfig.getLocation(),
                                                                   StreamUtils.getGeneration(streamConfig));

        LOG.info("Using Stream as input from {}", streamPath.toURI());

        TextStreamInputFormat.setTTL(job, streamConfig.getTTL());
        TextStreamInputFormat.setStreamPath(job, streamPath.toURI());
        TextStreamInputFormat.setTimeRange(job, stream.getStartTime(), stream.getEndTime());
        job.setInputFormatClass(TextStreamInputFormat.class);

      } else {
        // We checked on validation phase that it implements BatchReadable
        BatchReadable inputDataSet = (BatchReadable) context.getDataSet(inputDataSetName);
        List<Split> inputSplits = context.getInputDataSelection();
        if (inputSplits == null) {
          inputSplits = inputDataSet.getSplits();
        }
        context.setInput(inputDataSetName, inputSplits);

        LOG.debug("Using Dataset {} as input for MapReduce Job", inputDataSetName);
        DataSetInputFormat.setInput(job, inputDataSetName);
      }
    }
  }

  /**
   * Sets the configurations for Dataset used for output.
   */
  private void setOutputDataSetIfNeeded(Job job) {
    String outputDataSetName = context.getOutputDatasetName();

    // whatever was set into mapReduceContext e.g. during beforeSubmit(..) takes precedence
    if (outputDataSetName == null) {
      // trying to init output dataset from spec
      outputDataSetName = context.getSpecification().getOutputDataSet();
    }

    if (outputDataSetName != null) {
      LOG.debug("Using Dataset {} as output for MapReduce Job", outputDataSetName);
      DataSetOutputFormat.setOutput(job, outputDataSetName);
    }
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
    // Fastutil - 16MB library that only used in tehpra server
    ApplicationBundler appBundler = new ApplicationBundler(ImmutableList.of("org.apache.hadoop",
                                                                            "org.apache.spark",
                                                                            "it.unimi.dsi.fastutil"),
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
    classes.add(DataSetOutputFormat.class);
    classes.add(DataSetInputFormat.class);
    classes.add(TextStreamInputFormat.class);
    classes.add(MapperWrapper.class);
    classes.add(ReducerWrapper.class);

    Job jobConf = context.getHadoopJob();
    try {
      Class<? extends InputFormat<?, ?>> inputFormatClass = jobConf.getInputFormatClass();
      LOG.info("InputFormat class: {} {}", inputFormatClass, inputFormatClass.getClassLoader());
      classes.add(inputFormatClass);
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
