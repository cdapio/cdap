/*
 * Copyright 2014 Cask, Inc.
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
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.lang.CombineClassLoader;
import co.cask.cdap.common.lang.PropertyFieldSetter;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.common.LogWriter;
import co.cask.cdap.common.logging.logback.CAppender;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.data.dataset.DataSetInstantiator;
import co.cask.cdap.data.dataset.DatasetCreationSpec;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data.stream.TextStreamInputFormat;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.DataSetFieldSetter;
import co.cask.cdap.internal.app.runtime.DataSets;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramServiceDiscovery;
import co.cask.cdap.internal.app.runtime.batch.dataset.DataSetInputFormat;
import co.cask.cdap.internal.app.runtime.batch.dataset.DataSetOutputFormat;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.continuuity.tephra.Transaction;
import com.continuuity.tephra.TransactionExecutor;
import com.continuuity.tephra.TransactionExecutorFactory;
import com.continuuity.tephra.TransactionFailureException;
import com.continuuity.tephra.TransactionSystemClient;
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
import java.net.URI;
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

    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == ProgramType.MAPREDUCE, "Only MAPREDUCE process type is supported.");

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

    DataSetInstantiator dataSetInstantiator = new DataSetInstantiator(datasetFramework,
                                                                      cConf, program.getClassLoader());
    Map<String, DatasetCreationSpec> datasetSpecs = program.getSpecification().getDatasets();
    dataSetInstantiator.setDataSets(datasetSpecs.values());

    Map<String, Closeable> dataSets = DataSets.createDataSets(dataSetInstantiator, datasetSpecs.keySet());

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

      LOG.info("Starting MapReduce Job: {}", context.toString());
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
        LOG.info("Stopping MapReduce Job: {}", context);
        try {
          if (!jobConf.isComplete()) {
            jobConf.killJob();
          }
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
        LOG.info("MapReduce Job stopped: {}", context);
      }
    }, MoreExecutors.sameThreadExecutor());

    return controller;
  }

  private void submit(final MapReduce job, MapReduceSpecification mapredSpec, Location jobJarLocation,
                      final BasicMapReduceContext context,
                      final DataSetInstantiator dataSetInstantiator) throws Exception {
    jobConf = Job.getInstance(new Configuration(hConf));
    Configuration mapredConf = jobConf.getConfiguration();

    if (UserGroupInformation.isSecurityEnabled()) {
      // If runs in secure cluster, this program runner is running in a yarn container, hence not able
      // to get authenticated with the history and MR-AM.
      mapredConf.unset("mapreduce.jobhistory.address");
      mapredConf.setBoolean(MRJobConfig.JOB_AM_ACCESS_DISABLED, true);
    }

    int mapperMemory = mapredSpec.getMapperMemoryMB();
    int reducerMemory = mapredSpec.getReducerMemoryMB();
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
      Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
      LOG.info("Running in secure mode; adding all user credentials: {}", credentials.getAllTokens());
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
    LOG.info("Built MapReduce Job Jar at {}", jobJar.toURI().toString());
    final Location programJarCopy = createJobJarTempCopy(jobJarLocation, context);
    LOG.info("Copied Program Jar to {}, source: {}", programJarCopy.toURI().toString(),
             jobJarLocation.toURI().toString());
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
            LOG.info("Submitting MapReduce Job: {}", context.toString());

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

            LOG.info("MapReduce Job is complete, status: {}, success: {}, job: {}" + jobConf.getStatus(), success, 
                     context.toString());
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
          LOG.warn("Received Exception after submitting MapReduce Job", e);
          throw Throwables.propagate(e);
        } finally {
          // stopping controller when mapreduce job is finished
          stopController(success, context, job, tx, dataSetInstantiator);
          try {
            jobJar.delete();
          } catch (IOException e) {
            LOG.warn("Failed to delete temp MapReduce Job Jar: {}", jobJar.toURI());
            // failure should not affect other stuff
          }
          try {
            programJarCopy.delete();
          } catch (IOException e) {
            LOG.warn("Failed to delete MapReduce Job Jar: {}", programJarCopy.toURI());
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
                                         ProgramType.MAPREDUCE.name().toLowerCase(),
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
        LOG.warn("Exception from stopping controller: {}", context, e);
        // we ignore the exception because we don't really care about the controller, but we must end the transaction!
      }
      try {
        try {
          if (success) {
            // committing long running tx: no need to commit datasets, as they were committed in external processes
            // also no need to rollback changes if commit fails, as these changes where performed by mapreduce tasks
            // NOTE: can't call afterCommit on datasets in this case: the changes were made by external processes.
            if (!txSystemClient.commit(tx)) {
              LOG.warn("MapReduce Job transaction failed to commit");
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
    String outputDataSetName;
    // whatever was set into mapReduceContext e.g. during beforeSubmit(..) takes precedence
    if (mapReduceContext.getOutputDatasetName() != null) {
      outputDataSetName = mapReduceContext.getOutputDatasetName();
    } else {
      // trying to init output dataset from spec
      outputDataSetName = mapReduceContext.getSpecification().getOutputDataSet();
    }

    if (outputDataSetName != null) {
      LOG.debug("Using Dataset {} as output for MapReduce Job", outputDataSetName);
      DataSetOutputFormat.setOutput(jobConf, outputDataSetName);
    }
  }

  @SuppressWarnings("unchecked")
  private void setInputDataSetIfNeeded(Job jobConf, BasicMapReduceContext mapReduceContext) throws IOException {
    String inputDataSetName;
    if (mapReduceContext.getInputDatasetName() != null) {
      inputDataSetName = mapReduceContext.getInputDatasetName();
    } else  {
      // trying to init input dataset from spec
      inputDataSetName = mapReduceContext.getSpecification().getInputDataSet();
    }

    if (inputDataSetName != null) {
      // TODO: It's a hack for stream
      if (inputDataSetName.startsWith("stream://")) {
        StreamBatchReadable stream = new StreamBatchReadable(URI.create(inputDataSetName));
        StreamConfig streamConfig = streamAdmin.getConfig(stream.getStreamName());
        Location streamPath = StreamUtils.createGenerationLocation(streamConfig.getLocation(),
                                                                   StreamUtils.getGeneration(streamConfig));

        LOG.info("Using Stream as input from {}", streamPath.toURI());

        TextStreamInputFormat.setTTL(jobConf, streamConfig.getTTL());
        TextStreamInputFormat.setStreamPath(jobConf, streamPath.toURI());
        TextStreamInputFormat.setTimeRange(jobConf, stream.getStartTime(), stream.getEndTime());
        jobConf.setInputFormatClass(TextStreamInputFormat.class);

      } else {
        // We checked on validation phase that it implements BatchReadable
        BatchReadable inputDataSet = (BatchReadable) mapReduceContext.getDataSet(inputDataSetName);
        List<Split> inputSplits = mapReduceContext.getInputDataSelection();
        if (inputSplits == null) {
          inputSplits = inputDataSet.getSplits();
        }
        mapReduceContext.setInput(inputDataSetName, inputSplits);

        LOG.debug("Using Dataset {} as input for MapReduce Job", inputDataSetName);
        DataSetInputFormat.setInput(jobConf, inputDataSetName);
      }
    }
  }

  private Location buildJobJar(BasicMapReduceContext context) throws IOException {
    ApplicationBundler appBundler = new ApplicationBundler(Lists.newArrayList("org.apache.hadoop"),
                                                           Lists.newArrayList("org.apache.hadoop.hbase",
                                                                              "org.apache.hadoop.hive"));
    Id.Program programId = context.getProgram().getId();

    Location appFabricDependenciesJarLocation =
      locationFactory.create(String.format("%s.%s.%s.%s.%s.jar",
                                           ProgramType.MAPREDUCE.name().toLowerCase(),
                                           programId.getAccountId(), programId.getApplicationId(),
                                           programId.getId(), context.getRunId().getId()));

    LOG.debug("Creating Job jar: {}", appFabricDependenciesJarLocation.toURI());

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
    appBundler.createBundle(appFabricDependenciesJarLocation, classes);
    Thread.currentThread().setContextClassLoader(oldCLassLoader);

    return appFabricDependenciesJarLocation;
  }
}
