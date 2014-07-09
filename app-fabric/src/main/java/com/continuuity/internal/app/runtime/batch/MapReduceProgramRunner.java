package com.continuuity.internal.app.runtime.batch;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.stream.StreamBatchReadable;
import com.continuuity.api.dataset.Dataset;
import com.continuuity.api.dataset.lib.AbstractDataset;
import com.continuuity.api.mapreduce.MapReduce;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.lang.CombineClassLoader;
import com.continuuity.common.lang.PropertyFieldSetter;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.common.logging.common.LogWriter;
import com.continuuity.common.logging.logback.CAppender;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.data.DataFabric;
import com.continuuity.data.DataFabric2Impl;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.dataset.DataSetInstantiator;
import com.continuuity.data.dataset.DatasetCreationSpec;
import com.continuuity.data.stream.StreamUtils;
import com.continuuity.data.stream.TextStreamInputFormat;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.continuuity.data2.transaction.TransactionFailureException;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.data2.transaction.stream.StreamConfig;
import com.continuuity.data2.util.hbase.HBaseTableUtilFactory;
import com.continuuity.internal.app.runtime.AbstractListener;
import com.continuuity.internal.app.runtime.DataSetFieldSetter;
import com.continuuity.internal.app.runtime.DataSets;
import com.continuuity.internal.app.runtime.ProgramOptionConstants;
import com.continuuity.internal.app.runtime.ProgramServiceDiscovery;
import com.continuuity.internal.app.runtime.batch.dataset.DataSetInputFormat;
import com.continuuity.internal.app.runtime.batch.dataset.DataSetOutputFormat;
import com.continuuity.internal.lang.Reflections;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
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
import org.apache.twill.api.RunId;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.ApplicationBundler;
import org.apache.twill.internal.RunIds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Runs {@link MapReduce} programs.
 */
public class MapReduceProgramRunner implements ProgramRunner {
  private static final Logger LOG = LoggerFactory.getLogger(MapReduceProgramRunner.class);

  private final StreamAdmin streamAdmin;
  private final CConfiguration cConf;
  private final Configuration hConf;
  private final LocationFactory locationFactory;
  private final MetricsCollectionService metricsCollectionService;
  private final DataSetAccessor dataSetAccessor;
  private final DatasetFramework datasetFramework;

  private final TransactionSystemClient txSystemClient;
  private final TransactionExecutorFactory txExecutorFactory;
  private final ProgramServiceDiscovery serviceDiscovery;

  private Job jobConf;
  private MapReduceProgramController controller;

  @Inject
  public MapReduceProgramRunner(CConfiguration cConf, Configuration hConf,
                                LocationFactory locationFactory,
                                StreamAdmin streamAdmin,
                                DataSetAccessor dataSetAccessor,
                                DatasetFramework datasetFramework,
                                TransactionSystemClient txSystemClient,
                                MetricsCollectionService metricsCollectionService,
                                TransactionExecutorFactory txExecutorFactory,
                                ProgramServiceDiscovery serviceDiscovery) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.locationFactory = locationFactory;
    this.streamAdmin = streamAdmin;
    this.metricsCollectionService = metricsCollectionService;
    this.dataSetAccessor = dataSetAccessor;
    this.datasetFramework = datasetFramework;
    this.txSystemClient = txSystemClient;
    this.txExecutorFactory = txExecutorFactory;
    this.serviceDiscovery = serviceDiscovery;
  }

  @Inject (optional = true)
  void setLogWriter(@Nullable LogWriter logWriter) {
    if (logWriter != null) {
      CAppender.logWriter = logWriter;
    }
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    Type processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == Type.MAPREDUCE, "Only MAPREDUCE process type is supported.");

    MapReduceSpecification spec = appSpec.getMapReduce().get(program.getName());
    Preconditions.checkNotNull(spec, "Missing MapReduceSpecification for %s", program.getName());

    // Optionally get runId. If the map-reduce started by other program (e.g. Workflow), it inherit the runId.
    Arguments arguments = options.getArguments();
    RunId runId = arguments.hasOption(ProgramOptionConstants.RUN_ID)
                    ? RunIds.fromString(arguments.getOption(ProgramOptionConstants.RUN_ID))
                    : RunIds.generate();

    long logicalStartTime = arguments.hasOption(ProgramOptionConstants.LOGICAL_START_TIME)
                                ? Long.parseLong(arguments
                                                   .getOption(ProgramOptionConstants.LOGICAL_START_TIME))
                                : System.currentTimeMillis();

    String workflowBatch = arguments.getOption(ProgramOptionConstants.WORKFLOW_BATCH);

    DataFabric dataFabric = new DataFabric2Impl(locationFactory, dataSetAccessor);
    DataSetInstantiator dataSetInstantiator = new DataSetInstantiator(dataFabric, datasetFramework,
                                                                      cConf, program.getClassLoader());
    Map<String, DataSetSpecification> dataSetSpecs = program.getSpecification().getDataSets();
    Map<String, DatasetCreationSpec> datasetSpecs = program.getSpecification().getDatasets();
    dataSetInstantiator.setDataSets(dataSetSpecs.values(), datasetSpecs.values());

    Map<String, Closeable> dataSets = DataSets.createDataSets(dataSetInstantiator,
                                                              Sets.union(dataSetSpecs.keySet(), datasetSpecs.keySet()));

    final BasicMapReduceContext context =
      new BasicMapReduceContext(program, null, runId, options.getUserArguments(),
                                dataSets, spec,
                                dataSetInstantiator.getTransactionAware(),
                                logicalStartTime,
                                workflowBatch, serviceDiscovery, metricsCollectionService);

    try {
      MapReduce job = program.<MapReduce>getMainClass().newInstance();

      Reflections.visit(job, TypeToken.of(job.getClass()),
                        new PropertyFieldSetter(context.getSpecification().getProperties()),
                        new DataSetFieldSetter(context));

      // note: this sets logging context on the thread level
      LoggingContextAccessor.setLoggingContext(context.getLoggingContext());

      controller = new MapReduceProgramController(context);

      LOG.info("Starting MapReduce job: " + context.toString());
      submit(job, spec, program.getJarLocation(), context, dataSetInstantiator);

    } catch (Throwable e) {
      // failed before job even started - release all resources of the context
      context.close();
      throw Throwables.propagate(e);
    }

    // adding listener which stops mapreduce job when controller stops.
    controller.addListener(new AbstractListener() {
      @Override
      public void stopping() {
        LOG.info("Stopping mapreduce job: " + context);
        try {
          if (!jobConf.isComplete()) {
            jobConf.killJob();
          }
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
        LOG.info("Mapreduce job stopped: " + context);
      }
    }, MoreExecutors.sameThreadExecutor());

    return controller;
  }

  private void submit(final MapReduce job, MapReduceSpecification mapredSpec, Location jobJarLocation,
                      final BasicMapReduceContext context,
                      final DataSetInstantiator dataSetInstantiator) throws Exception {
    Configuration mapredConf = new Configuration(hConf);

    if (UserGroupInformation.isSecurityEnabled()) {
      // If runs in secure cluster, this program runner is running in a yarn container, hence not able
      // to get authenticated with the history and MR-AM.
      mapredConf.unset("mapreduce.jobhistory.address");
      mapredConf.setBoolean(MRJobConfig.JOB_AM_ACCESS_DISABLED, true);
    }

    int mapperMemory = mapredSpec.getMapperMemoryMB();
    int reducerMemory = mapredSpec.getReducerMemoryMB();
    // this will determine how much memory the yarn container will run with
    mapredConf.setInt("mapreduce.map.memory.mb", mapperMemory);
    mapredConf.setInt("mapreduce.reduce.memory.mb", reducerMemory);
    // java heap size doesn't automatically get set to the yarn container memory...
    mapredConf.set("mapreduce.map.java.opts", "-Xmx" + mapperMemory + "m");
    mapredConf.set("mapreduce.reduce.java.opts", "-Xmx" + reducerMemory + "m");
    jobConf = Job.getInstance(mapredConf);

    if (UserGroupInformation.isSecurityEnabled()) {
      Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
      LOG.info("Running in secure mode. Adding all user credentials: {}", credentials.getAllTokens());
      jobConf.getCredentials().addAll(credentials);
    }

    // Create a classloader that have the context/system classloader as parent and the program classloader as child
    ClassLoader classLoader = new CombineClassLoader(
      Objects.firstNonNull(Thread.currentThread().getContextClassLoader(), ClassLoader.getSystemClassLoader()),
      ImmutableList.of(context.getProgram().getClassLoader())
    );

    jobConf.getConfiguration().setClassLoader(classLoader);
    context.setJob(jobConf);

    // additional mapreduce job initialization at run-time
    beforeSubmit(job, context, dataSetInstantiator);

    // replace user's Mapper & Reducer's with our wrappers in job config
    wrapMapperClassIfNeeded(jobConf);
    wrapReducerClassIfNeeded(jobConf);

    // set input/output datasets info
    setInputDataSetIfNeeded(jobConf, context);
    setOutputDataSetIfNeeded(jobConf, context);

    // packaging job jar which includes continuuity classes with dependencies
    // NOTE: user's jar is added to classpath separately to leave the flexibility in future to create and use separate
    //       classloader when executing user code. We need to submit a copy of the program jar because
    //       in distributed mode this returns program path on HDFS, not localized, which may cause race conditions
    //       if we allow deploying new program while existing is running. To prevent races we submit a temp copy

    final Location jobJar = buildJobJar(context);
    LOG.info("built jobJar at " + jobJar.toURI().toString());
    final Location programJarCopy = createJobJarTempCopy(jobJarLocation, context);
    LOG.info("copied programJar to " + programJarCopy.toURI().toString() +
               ", source: " + jobJarLocation.toURI().toString());

    jobConf.setJar(jobJar.toURI().toString());
    jobConf.addFileToClassPath(new Path(programJarCopy.toURI()));

    MapReduceContextConfig contextConfig = new MapReduceContextConfig(jobConf);
    // We start long-running tx to be used by mapreduce job tasks.
    final Transaction tx = txSystemClient.startLong();
    // We remember tx, so that we can re-use it in mapreduce tasks
    contextConfig.set(context, cConf, tx, programJarCopy.getName());

    new Thread() {
      @Override
      public void run() {
        boolean success = false;
        try {
          // note: this sets logging context on the thread level
          LoggingContextAccessor.setLoggingContext(context.getLoggingContext());
          try {
            LOG.info("Submitting mapreduce job {}", context.toString());

            ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(jobConf.getConfiguration().getClassLoader());
            try {
              // submits job and returns immediately
              jobConf.submit();
            } finally {
              Thread.currentThread().setContextClassLoader(oldClassLoader);
            }

            MapReduceMetricsWriter metricsWriter = new MapReduceMetricsWriter(jobConf, context);

            // until job is complete report stats
            while (!jobConf.isComplete()) {
              metricsWriter.reportStats();

              // we report to metrics backend every second, so 1 sec is enough here. That's mapreduce job anyways (not
              // short) ;)
              TimeUnit.MILLISECONDS.sleep(1000);
            }

            LOG.info("Job is complete, status: " + jobConf.getStatus() +
                       ", success: " + success +
                       ", job: " + context.toString());

            // NOTE: we want to report the final stats (they may change since last report and before job completed)
            metricsWriter.reportStats();
            // If we don't sleep, the final stats may not get sent before shutdown.
            TimeUnit.SECONDS.sleep(2L);

            success = jobConf.isSuccessful();
          } catch (InterruptedException e) {
            // nothing we can do now: we simply stopped watching for job completion...
            throw Throwables.propagate(e);
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }

        } catch (Exception e) {
          LOG.warn("Received Exception after submitting MapReduce job.", e);
          throw Throwables.propagate(e);
        } finally {
          // stopping controller when mapreduce job is finished
          stopController(success, context, job, tx, dataSetInstantiator);
          try {
            jobJar.delete();
          } catch (IOException e) {
            LOG.warn("Failed to delete temp mr job jar: " + jobJar.toURI());
            // failure should not affect other stuff
          }
          try {
            programJarCopy.delete();
          } catch (IOException e) {
            LOG.warn("Failed to delete temp mr job jar: " + programJarCopy.toURI());
            // failure should not affect other stuff
          }
        }
      }
    }.start();
  }

  private void beforeSubmit(final MapReduce job,
                            final BasicMapReduceContext context,
                            final DataSetInstantiator dataSetInstantiator)
    throws TransactionFailureException, InterruptedException {
    TransactionExecutor txExecutor = txExecutorFactory.createExecutor(dataSetInstantiator.getTransactionAware());
    // TODO: retry on txFailure or txConflict? Implement retrying TransactionExecutor
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(job.getClass().getClassLoader());
        try {
          job.beforeSubmit(context);
        } finally {
          Thread.currentThread().setContextClassLoader(oldClassLoader);
        }
      }
    });
  }

  private void onFinish(final MapReduce job,
                        final BasicMapReduceContext context,
                        final DataSetInstantiator dataSetInstantiator,
                        final boolean succeeded)
    throws TransactionFailureException, InterruptedException {
    TransactionExecutor txExecutor = txExecutorFactory.createExecutor(dataSetInstantiator.getTransactionAware());
    // TODO: retry on txFailure or txConflict? Implement retrying TransactionExecutor
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        job.onFinish(succeeded, context);
      }
    });
  }

  private Location createJobJarTempCopy(Location jobJarLocation, BasicMapReduceContext context) throws IOException {

    Id.Program programId = context.getProgram().getId();
    Location programJarCopy = locationFactory.create(String.format("%s.%s.%s.%s.%s.program.jar",
                                         Type.MAPREDUCE.name().toLowerCase(),
                                         programId.getAccountId(), programId.getApplicationId(),
                                         programId.getId(), context.getRunId().getId()));
    InputStream src = jobJarLocation.getInputStream();
    try {
      OutputStream dest = programJarCopy.getOutputStream();
      try {
        ByteStreams.copy(src, dest);
      } finally {
        dest.close();
      }
    } finally {
      src.close();
    }
    return programJarCopy;
  }

  private void wrapMapperClassIfNeeded(Job jobConf) throws ClassNotFoundException {
    // NOTE: we don't use jobConf.getMapperClass() as we cannot (and don't want to) access user class here
    String mapClass = jobConf.getConfiguration().get(MRJobConfig.MAP_CLASS_ATTR);
    if (mapClass != null) {
      jobConf.getConfiguration().set(MapperWrapper.ATTR_MAPPER_CLASS, mapClass);
      // yes, it is a subclass of Mapper
      @SuppressWarnings("unchecked")
      Class<? extends Mapper> wrapperClass = MapperWrapper.class;
      jobConf.setMapperClass(wrapperClass);
    }
  }

  private void wrapReducerClassIfNeeded(Job jobConf) throws ClassNotFoundException {
    // NOTE: we don't use jobConf.getReducerClass() as we cannot (and don't want to) access user class here
    String reducerClass = jobConf.getConfiguration().get(MRJobConfig.REDUCE_CLASS_ATTR);
    if (reducerClass != null) {
      jobConf.getConfiguration().set(ReducerWrapper.ATTR_REDUCER_CLASS, reducerClass);
      // yes, it is a subclass of Reducer
      @SuppressWarnings("unchecked")
      Class<? extends Reducer> wrapperClass = ReducerWrapper.class;
      jobConf.setReducerClass(wrapperClass);
    }
  }

  private void stopController(boolean success,
                              BasicMapReduceContext context,
                              MapReduce job,
                              Transaction tx,
                              DataSetInstantiator dataSetInstantiator) {
    try {
      try {
        controller.stop().get();
      } catch (Throwable e) {
        LOG.warn("Exception from stopping controller: " + context, e);
        // we ignore the exception because we don't really care about the controller, but we must end the transaction!
      }
      try {
        try {
          if (success) {
            // committing long running tx: no need to commit datasets, as they were committed in external processes
            // also no need to rollback changes if commit fails, as these changes where performed by mapreduce tasks
            // NOTE: can't call afterCommit on datasets in this case: the changes were made by external processes.
            if (!txSystemClient.commit(tx)) {
              LOG.warn("Mapreduce job transaction failed to commit");
              success = false;
            }
          } else {
            // aborting long running tx: no need to do rollbacks, etc.
            txSystemClient.abort(tx);
          }
        } finally {
          // whatever happens we want to call this
          onFinish(job, context, dataSetInstantiator, success);
        }
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    } finally {
      // release all resources, datasets, etc. of the context
      context.close();
    }
  }

  private void setOutputDataSetIfNeeded(Job jobConf, BasicMapReduceContext mapReduceContext) {
    String outputDataSetName = null;
    BatchWritable outputDataset;
    // whatever was set into mapReduceContext e.g. during beforeSubmit(..) takes precedence

    if (mapReduceContext.getOutputDatasetName() != null) {
      outputDataSetName = mapReduceContext.getOutputDatasetName();
    } else if (mapReduceContext.getOutputDataset() != null) {
      outputDataset = mapReduceContext.getOutputDataset();
      if (outputDataset instanceof DataSet) {
        outputDataSetName = ((DataSet) outputDataset).getName();
      }
    } else {
      // trying to init output dataset from spec
      outputDataSetName = mapReduceContext.getSpecification().getOutputDataSet();
    }

    if (outputDataSetName != null) {
      LOG.debug("Using dataset {} as output for mapreduce job", outputDataSetName);
      // We checked on validation phase that it implements BatchWritable
      outputDataset = (BatchWritable) mapReduceContext.getDataSet(outputDataSetName);
      mapReduceContext.setOutput(outputDataset);
      DataSetOutputFormat.setOutput(jobConf, outputDataSetName);
    }
  }

  @SuppressWarnings("unchecked")
  private void setInputDataSetIfNeeded(Job jobConf, BasicMapReduceContext mapReduceContext) throws IOException {
    String inputDataSetName;
    // whatever was set into mapReduceJob e.g. during beforeSubmit(..) takes precedence
    BatchReadable batchReadable = mapReduceContext.getInputDataset();

    if (mapReduceContext.getInputDatasetName() != null) {
      inputDataSetName = mapReduceContext.getInputDatasetName();
    } else if (batchReadable != null && batchReadable instanceof DataSet) {
      inputDataSetName = ((DataSet) batchReadable).getName();
    } else  {
      // trying to init input dataset from spec
      inputDataSetName = mapReduceContext.getSpecification().getInputDataSet();
    }

    if (inputDataSetName != null) {
      // TODO: It's a hack for stream
      if (inputDataSetName.startsWith("stream://")) {
        batchReadable = new StreamBatchReadable(inputDataSetName.substring("stream://".length()));
      } else {
        // We checked on validation phase that it implements BatchReadable
        BatchReadable inputDataSet = (BatchReadable) mapReduceContext.getDataSet(inputDataSetName);
        List<Split> inputSplits = mapReduceContext.getInputDataSelection();
        if (inputSplits == null) {
          inputSplits = inputDataSet.getSplits();
        }
        mapReduceContext.setInput(inputDataSet, inputSplits);
      }
    }

    if (inputDataSetName != null) {
      LOG.debug("Using dataset {} as input for mapreduce job", inputDataSetName);
      DataSetInputFormat.setInput(jobConf, inputDataSetName);
    } else if (batchReadable instanceof StreamBatchReadable) {
      // TODO: It's a hack for stream
      StreamBatchReadable stream = (StreamBatchReadable) batchReadable;
      StreamConfig streamConfig = streamAdmin.getConfig(stream.getStreamName());
      Location streamPath = StreamUtils.createGenerationLocation(streamConfig.getLocation(),
                                                                 StreamUtils.getGeneration(streamConfig));

      LOG.info("Using stream as input from {}", streamPath.toURI());

      TextStreamInputFormat.setTTL(jobConf, streamConfig.getTTL());
      TextStreamInputFormat.setStreamPath(jobConf, streamPath.toURI());
      TextStreamInputFormat.setTimeRange(jobConf, stream.getStartTime(), stream.getEndTime());
      jobConf.setInputFormatClass(TextStreamInputFormat.class);
    }
  }

  private Location buildJobJar(BasicMapReduceContext context) throws IOException {
    ApplicationBundler appBundler = new ApplicationBundler(Lists.newArrayList("org.apache.hadoop"),
                                                           Lists.newArrayList("org.apache.hadoop.hbase",
                                                                              "org.apache.hadoop.hive"));
    Id.Program programId = context.getProgram().getId();

    Location appFabricDependenciesJarLocation =
      locationFactory.create(String.format("%s.%s.%s.%s.%s.jar",
                                           Type.MAPREDUCE.name().toLowerCase(),
                                           programId.getAccountId(), programId.getApplicationId(),
                                           programId.getId(), context.getRunId().getId()));

    LOG.debug("Creating job jar: {}", appFabricDependenciesJarLocation.toURI());

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
      LOG.warn("Not including HBaseTableUtil classes in submitted job jar since they are not available.");
    }

    ClassLoader oldCLassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(jobConf.getConfiguration().getClassLoader());
    appBundler.createBundle(appFabricDependenciesJarLocation, classes);
    Thread.currentThread().setContextClassLoader(oldCLassLoader);

    return appFabricDependenciesJarLocation;
  }
}
