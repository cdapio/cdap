/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.Injector;
import com.google.inject.ProvisionException;
import io.cdap.cdap.api.ProgramLifecycle;
import io.cdap.cdap.api.ProgramState;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.Transactionals;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.mapreduce.AbstractMapReduce;
import io.cdap.cdap.api.mapreduce.MapReduce;
import io.cdap.cdap.api.mapreduce.MapReduceContext;
import io.cdap.cdap.api.mapreduce.MapReduceSpecification;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.CConfigurationUtil;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.common.twill.ProgramRuntimeClassAcceptor;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.data2.metadata.lineage.field.FieldLineageInfo;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.transaction.Transactions;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtilFactory;
import io.cdap.cdap.internal.app.runtime.LocalizationUtils;
import io.cdap.cdap.internal.app.runtime.ProgramRunners;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.batch.dataset.input.MapperInput;
import io.cdap.cdap.internal.app.runtime.batch.dataset.input.MultipleInputs;
import io.cdap.cdap.internal.app.runtime.batch.dataset.output.MultipleOutputsMainOutputWrapper;
import io.cdap.cdap.internal.app.runtime.batch.dataset.output.ProvidedOutput;
import io.cdap.cdap.internal.app.runtime.batch.distributed.ContainerLauncherGenerator;
import io.cdap.cdap.internal.app.runtime.batch.distributed.MapReduceContainerHelper;
import io.cdap.cdap.internal.app.runtime.batch.distributed.MapReduceContainerLauncher;
import io.cdap.cdap.internal.app.runtime.distributed.LocalizeResource;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.security.store.SecureStoreUtils;
import io.cdap.cdap.spi.hbase.HBaseDDLExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tephra.TransactionFailureException;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.api.Configs;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.ApplicationBundler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.Path;

/**
 * Performs the actual execution of mapreduce job.
 *
 * Service start -> Performs job setup, initialize and submit job
 * Service run -> Poll for job completion
 * Service triggerStop -> kill job
 * Service stop -> destroy, cleanup
 */
final class MapReduceRuntimeService extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceRuntimeService.class);
  private static final String HADOOP_UMASK_PROPERTY = FsPermission.UMASK_LABEL; // fs.permissions.umask-mode

  /**
   * Do not remove: we need this variable for loading MRClientSecurityInfo class required for communicating with
   * AM in secure mode.
   */
  @SuppressWarnings("unused")
  private org.apache.hadoop.mapreduce.v2.app.MRClientSecurityInfo mrClientSecurityInfo;

  private final Injector injector;
  private final CConfiguration cConf;
  private final Configuration hConf;
  private final MapReduce mapReduce;
  private final MapReduceSpecification specification;
  private final Location programJarLocation;
  private final BasicMapReduceContext context;
  private final NamespacePathLocator locationFactory;
  private final ProgramLifecycle<MapReduceContext> programLifecycle;
  private final ProgramRunId mapReduceRunId;
  private final ClusterMode clusterMode;
  private final FieldLineageWriter fieldLineageWriter;

  private Job job;
  private Runnable cleanupTask;

  private volatile boolean stopRequested;

  MapReduceRuntimeService(Injector injector, CConfiguration cConf, Configuration hConf,
                          final MapReduce mapReduce, MapReduceSpecification specification,
                          final BasicMapReduceContext context, Location programJarLocation,
                          NamespacePathLocator locationFactory, ClusterMode clusterMode,
                          FieldLineageWriter fieldLineageWriter) {
    this.injector = injector;
    this.cConf = cConf;
    this.hConf = hConf;
    this.mapReduce = mapReduce;
    this.specification = specification;
    this.programJarLocation = programJarLocation;
    this.locationFactory = locationFactory;
    this.context = context;
    this.programLifecycle = new ProgramLifecycle<MapReduceContext>() {
      @Override
      public void initialize(MapReduceContext context) throws Exception {
        if (mapReduce instanceof ProgramLifecycle) {
          //noinspection unchecked
          ((ProgramLifecycle) mapReduce).initialize(context);
        }
      }

      @Override
      public void destroy() {
        if (mapReduce instanceof ProgramLifecycle) {
          //noinspection unchecked
          ((ProgramLifecycle) mapReduce).destroy();
        }
      }
    };
    this.mapReduceRunId = context.getProgram().getId().run(context.getRunId().getId());
    this.clusterMode = clusterMode;
    this.fieldLineageWriter = fieldLineageWriter;
  }

  @Override
  protected String serviceName() {
    return "MapReduceRunner-" + specification.getName();
  }

  @Override
  protected void startUp() throws Exception {
    // Creates a temporary directory locally for storing all generated files.
    File tempDir = createTempDirectory();
    cleanupTask = createCleanupTask(tempDir, context);

    try {
      Job job = createJob(new File(tempDir, "mapreduce"));
      Configuration mapredConf = job.getConfiguration();

      MapReduceClassLoader classLoader = new MapReduceClassLoader(injector, cConf, mapredConf,
                                                                  context.getProgram().getClassLoader(),
                                                                  context.getApplicationSpecification().getPlugins(),
                                                                  context.getPluginInstantiator());
      cleanupTask = createCleanupTask(classLoader, cleanupTask);
      context.setMapReduceClassLoader(classLoader);
      context.setJob(job);

      mapredConf.setClassLoader(context.getProgramInvocationClassLoader());

      // we will now call initialize(), hence destroy() must be called on shutdown/cleanup
      cleanupTask = createCleanupTask((Runnable) this::destroy, cleanupTask);
      beforeSubmit(job);

      // Localize additional resources that users have requested via BasicMapReduceContext.localize methods
      Map<String, String> localizedUserResources = localizeUserResources(job, tempDir);

      // Override user-defined job name, since we set it and depend on the name.
      // https://issues.cask.co/browse/CDAP-2441
      String jobName = job.getJobName();
      if (!jobName.isEmpty()) {
        LOG.warn("Job name {} is being overridden.", jobName);
      }
      job.setJobName(getJobName(context));

      // Create a temporary location for storing all generated files through the LocationFactory.
      Location tempLocation = createTempLocationDirectory();
      cleanupTask = createCleanupTask(tempLocation, cleanupTask);

      // For local mode, everything is in the configuration classloader already, hence no need to create new jar
      if (!MapReduceTaskContextProvider.isLocal(mapredConf)) {
        // After calling initialize, we know what plugins are needed for the program, hence construct the proper
        // ClassLoader from here and use it for setting up the job
        Location pluginArchive = createPluginArchive(tempLocation);
        if (pluginArchive != null) {
          job.addCacheArchive(pluginArchive.toURI());
          mapredConf.set(Constants.Plugin.ARCHIVE, pluginArchive.getName());
        }
      }

      // set resources for the job
      TaskType.MAP.configure(mapredConf, cConf, context.getMapperRuntimeArguments(), context.getMapperResources());
      TaskType.REDUCE.configure(mapredConf, cConf, context.getReducerRuntimeArguments(), context.getReducerResources());

      // replace user's Mapper, Reducer, Partitioner, and Comparator classes with our wrappers in job config
      MapperWrapper.wrap(job);
      ReducerWrapper.wrap(job);
      PartitionerWrapper.wrap(job);
      RawComparatorWrapper.CombinerGroupComparatorWrapper.wrap(job);
      RawComparatorWrapper.GroupComparatorWrapper.wrap(job);
      RawComparatorWrapper.KeyComparatorWrapper.wrap(job);

      // packaging job jar which includes cdap classes with dependencies
      File jobJar = buildJobJar(job, tempDir);
      job.setJar(jobJar.toURI().toString());

      Location programJar = programJarLocation;
      String hbaseDDLExecutorDirectory = null;
      if (!MapReduceTaskContextProvider.isLocal(mapredConf)) {
        // Copy and localize the program jar in distributed mode
        programJar = copyProgramJar(tempLocation);
        job.addCacheFile(programJar.toURI());

        // Generate and localize the launcher jar to control the classloader of MapReduce containers processes
        Location launcherJar = createLauncherJar(tempLocation);
        job.addCacheFile(launcherJar.toURI());

        // Launcher.jar should be the first one in the classpath
        List<String> classpath = new ArrayList<>();
        classpath.add(launcherJar.getName());

        // Localize logback.xml
        Location logbackLocation = ProgramRunners.createLogbackJar(tempLocation.append("logback.xml.jar"));
        if (logbackLocation != null) {
          job.addCacheFile(logbackLocation.toURI());
          classpath.add(logbackLocation.getName());

          mapredConf.set("yarn.app.mapreduce.am.env", "CDAP_LOG_DIR=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
          mapredConf.set("mapreduce.map.env", "CDAP_LOG_DIR=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
          mapredConf.set("mapreduce.reduce.env", "CDAP_LOG_DIR=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
        }

        // (CDAP-7052) Locate the name of the javax.ws.rs-api jar to make it last in the classpath
        URL jaxrsURL = ClassLoaders.getClassPathURL(Path.class);
        String jaxrsClassPath = null;

        // Get all the jars in jobJar and sort them lexically before adding to the classpath
        // This allows CDAP classes to be picked up first before the Twill classes
        Set<String> jarFiles = new TreeSet<>();
        try (JarFile jobJarFile = new JarFile(jobJar)) {
          Enumeration<JarEntry> entries = jobJarFile.entries();
          while (entries.hasMoreElements()) {
            JarEntry entry = entries.nextElement();
            String entryName = entry.getName();
            if (entryName.startsWith("lib/") && entryName.endsWith(".jar")) {
              // Skip the jaxrs jar
              if (jaxrsURL != null && jaxrsURL.getPath().endsWith(entryName)) {
                jaxrsClassPath = "job.jar/" + entryName;
              } else {
                jarFiles.add("job.jar/" + entryName);
              }
            }
          }
        }

        classpath.addAll(jarFiles);
        classpath.add("job.jar/classes");

        // Add extra jars set in cConf
        for (URI jarURI : CConfigurationUtil.getExtraJars(cConf)) {
          if ("file".equals(jarURI.getScheme())) {
            Location extraJarLocation = copyFileToLocation(new File(jarURI.getPath()), tempLocation);
            job.addCacheFile(extraJarLocation.toURI());
          } else {
            job.addCacheFile(jarURI);
          }
          classpath.add(LocalizationUtils.getLocalizedName(jarURI));
        }

        hbaseDDLExecutorDirectory = getLocalizedHBaseDDLExecutorDir(tempDir, cConf, job, tempLocation);
        // Add the mapreduce application classpath, followed by the javax.ws.rs-api at last.
        MapReduceContainerHelper.addMapReduceClassPath(mapredConf, classpath);
        if (jaxrsClassPath != null) {
          classpath.add(jaxrsClassPath);
        }

        mapredConf.set(MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH, Joiner.on(",").join(classpath));
        mapredConf.set(YarnConfiguration.YARN_APPLICATION_CLASSPATH, Joiner.on(",").join(classpath));
      }

      MapReduceContextConfig contextConfig = new MapReduceContextConfig(mapredConf);
      CConfiguration cConfCopy = CConfiguration.copy(cConf);
      if (hbaseDDLExecutorDirectory != null) {
        cConfCopy.set(Constants.HBaseDDLExecutor.EXTENSIONS_DIR, hbaseDDLExecutorDirectory);
      }
      contextConfig.set(context, cConfCopy, programJar.toURI(), localizedUserResources);

      // submits job and returns immediately.
      // Set the context classloader to the program invocation one (which is a weak reference wrapped MRClassloader)
      ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(context.getProgramInvocationClassLoader());
      try {
        job.submit();
      } finally {
        ClassLoaders.setContextClassLoader(oldClassLoader);
      }
      // log after the job.submit(), because the jobId is not assigned before then
      LOG.info("Submitted MapReduce Job: {}.", context);

      this.job = job;
    } catch (Throwable t) {
      cleanupTask.run();
      if (t instanceof Error) {
        // Need to wrap Error. Otherwise, listeners of this Guava Service may not be called if the
        // initialization of the user program is missing dependencies (CDAP-2543).
        // Guava 15.0+ have this condition fixed, hence wrapping is no longer needed if upgrade to later Guava.
        throw new Exception(t);
      }

      // don't log the error. It will be logged by the ProgramControllerServiceAdapter.failed()
      if (t instanceof TransactionFailureException) {
        throw Transactionals.propagate((TransactionFailureException) t, Exception.class);
      }
      throw t;
    }
  }

  @Override
  protected void run() throws Exception {
    MapReduceMetricsWriter metricsWriter = new MapReduceMetricsWriter(job, context);
    long reportIntervalMillis = MapReduceMetricsUtil.getReportIntervalMillis(cConf, context.getRuntimeArguments());
    LOG.debug("Interval for reporting MapReduce stats is {} seconds.",
              TimeUnit.MILLISECONDS.toSeconds(reportIntervalMillis));

    long nextTimeToReport = 0L;

    // until job is complete report stats
    while (!job.isComplete()) {
      if (System.currentTimeMillis() >= nextTimeToReport) {
        // note: for a very large job, this may take several or even tens of seconds (it retrieves the task reports)
        metricsWriter.reportStats();
        nextTimeToReport = System.currentTimeMillis() + reportIntervalMillis;
      }
      // we want to poll for job completion frequently, but once a second should be fine for a long-running MR job
      TimeUnit.SECONDS.sleep(1);
    }

    // Job completed, set the final state.
    JobStatus jobStatus = job.getStatus();
    context.setState(getFinalProgramState(jobStatus));
    boolean success = context.getState().getStatus() == ProgramStatus.COMPLETED;

    LOG.info("MapReduce Job completed{}. Job details: [{}]", success ? " successfully" : "", context);

    // NOTE: we want to report the final stats (they may change since last report and before job completed)
    metricsWriter.reportStats();
    // If we don't sleep, the final stats may not get sent before shutdown.
    TimeUnit.SECONDS.sleep(2L);

    // If the job is not successful, throw exception so that this service will terminate with a failure state
    // Shutdown will still get executed, but the service will notify failure after that.
    // However, if it's the job is requested to stop (via triggerShutdown, meaning it's a user action), don't throw
    if (!stopRequested) {
      Preconditions.checkState(success, "MapReduce JobId %s failed", jobStatus.getJobID());
    }
  }

  @Override
  protected void shutDown() {
    cleanupTask.run();
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
        t.setName(serviceName());
        t.start();
      }
    };
  }

  /**
   * Creates a MapReduce {@link Job} instance.
   *
   * @param hadoopTmpDir directory for the "hadoop.tmp.dir" configuration
   */
  private Job createJob(File hadoopTmpDir) throws IOException {
    Job job = Job.getInstance(new Configuration(hConf));
    Configuration jobConf = job.getConfiguration();

    if (MapReduceTaskContextProvider.isLocal(jobConf)) {
      // Set the MR framework local directories inside the given tmp directory.
      // Setting "hadoop.tmp.dir" here has no effect due to Explore Service need to set "hadoop.tmp.dir"
      // as system property for Hive to work in local mode. The variable substitution of hadoop conf
      // gives system property the highest precedence.
      jobConf.set("mapreduce.cluster.local.dir", new File(hadoopTmpDir, "local").getAbsolutePath());
      jobConf.set("mapreduce.jobtracker.system.dir", new File(hadoopTmpDir, "system").getAbsolutePath());
      jobConf.set("mapreduce.jobtracker.staging.root.dir", new File(hadoopTmpDir, "staging").getAbsolutePath());
      jobConf.set("mapreduce.cluster.temp.dir", new File(hadoopTmpDir, "temp").getAbsolutePath());
    }

    if (UserGroupInformation.isSecurityEnabled()) {
      // If runs in secure cluster, this program runner is running in a yarn container, hence not able
      // to get authenticated with the history.
      jobConf.unset("mapreduce.jobhistory.address");
      jobConf.setBoolean(Job.JOB_AM_ACCESS_DISABLED, false);

      Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
      LOG.debug("Running in secure mode; adding all user credentials: {}", credentials.getAllTokens());
      job.getCredentials().addAll(credentials);
    }

    // Command-line arguments are not supported here, but do this anyway to avoid warning log
    GenericOptionsParser genericOptionsParser = new GenericOptionsParser(jobConf, null);
    genericOptionsParser.getRemainingArgs();

    return job;
  }

  /**
   * Creates a local temporary directory for this MapReduce run.
   */
  private File createTempDirectory() {
    ProgramId programId = context.getProgram().getId();
    File tempDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                            cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    File runtimeServiceDir = new File(tempDir, "runner");
    File dir = new File(runtimeServiceDir, String.format("%s.%s.%s.%s.%s",
                                                         programId.getType().name().toLowerCase(),
                                                         programId.getNamespace(), programId.getApplication(),
                                                         programId.getProgram(), context.getRunId().getId()));
    dir.mkdirs();
    return dir;
  }

  /**
   * Creates a temporary directory through the {@link LocationFactory} provided to this class.
   */
  private Location createTempLocationDirectory() throws IOException {
    ProgramId programId = context.getProgram().getId();

    String tempLocationName = String.format("%s/%s.%s.%s.%s.%s", cConf.get(Constants.AppFabric.TEMP_DIR),
                                            programId.getType().name().toLowerCase(),
                                            programId.getNamespace(), programId.getApplication(),
                                            programId.getProgram(), context.getRunId().getId());
    Location location = locationFactory.get(programId.getNamespaceId()).append(tempLocationName);
    location.mkdirs();
    return location;
  }

  /**
   * For MapReduce programs created after 3.5, calls the initialize method of the {@link ProgramLifecycle}.
   * This method also sets up the Input/Output within the same transaction.
   */
  private void beforeSubmit(final Job job) throws Exception {
    context.setState(new ProgramState(ProgramStatus.INITIALIZING, null));
    // AbstractMapReduce implements final initialize(context) and requires subclass to
    // implement initialize(), whereas programs that directly implement MapReduce have
    // the option to override initialize(context) (if they implement ProgramLifeCycle)
    TransactionControl defaultTxControl = context.getDefaultTxControl();
    TransactionControl txControl = mapReduce instanceof AbstractMapReduce
      ? Transactions.getTransactionControl(defaultTxControl, AbstractMapReduce.class,
                                           mapReduce, "initialize")
      : mapReduce instanceof ProgramLifecycle
      ? Transactions.getTransactionControl(defaultTxControl, MapReduce.class,
                                           mapReduce, "initialize", MapReduceContext.class)
      : defaultTxControl;

    context.initializeProgram(programLifecycle, txControl, false);

    // once the initialize method is executed, set the status of the MapReduce to RUNNING
    context.setState(new ProgramState(ProgramStatus.RUNNING, null));

    ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(context.getProgramInvocationClassLoader());
    try {
      // set input/outputs info, and get one of the configured mapper's TypeToken
      TypeToken<?> mapperTypeToken = setInputsIfNeeded(job);
      setOutputsIfNeeded(job);
      setOutputClassesIfNeeded(job, mapperTypeToken);
      setMapOutputClassesIfNeeded(job, mapperTypeToken);
    } finally {
      ClassLoaders.setContextClassLoader(oldClassLoader);
    }
  }

  /**
   * Returns the program state when the MR finished / killed.
   *
   * @param jobStatus the job status after the MR job completed
   * @return a {@link ProgramState}
   */
  private ProgramState getFinalProgramState(JobStatus jobStatus) {
    if (stopRequested) {
      // Program explicitly stopped, return KILLED state
      return new ProgramState(ProgramStatus.KILLED, null);
    }

    return (jobStatus.getState() == JobStatus.State.SUCCEEDED)
      ? new ProgramState(ProgramStatus.COMPLETED, null)
      : new ProgramState(ProgramStatus.FAILED, jobStatus.getFailureInfo());
  }

  /**
   * Calls the destroy method of {@link ProgramLifecycle}.
   */
  private void destroy() {
    TransactionControl defaultTxControl = context.getDefaultTxControl();
    TransactionControl txControl = mapReduce instanceof ProgramLifecycle
      ? Transactions.getTransactionControl(defaultTxControl, MapReduce.class, mapReduce, "destroy")
      : defaultTxControl;

    context.destroyProgram(programLifecycle, txControl, false);
    if (emitFieldLineage()) {
      try {
        // here we cannot call context.flushRecord() since the WorkflowNodeState will need to record and store
        // the lineage information
        FieldLineageInfo info = new FieldLineageInfo(context.getFieldLineageOperations());
        fieldLineageWriter.write(mapReduceRunId, info);
      } catch (Throwable t) {
        LOG.warn("Failed to emit the field lineage operations for MapReduce {}", mapReduceRunId, t);
      }
    }
  }

  private boolean emitFieldLineage() {
    if (context.getFieldLineageOperations().isEmpty()) {
      // no operations to emit
      return false;
    }

    ProgramStatus status = context.getState().getStatus();
    if (ProgramStatus.COMPLETED != status) {
      // MapReduce program failed, so field operations wont be emitted
      return false;
    }

    WorkflowProgramInfo workflowInfo = context.getWorkflowInfo();
    return workflowInfo == null || !workflowInfo.fieldLineageConsolidationEnabled();
  }

  private void assertConsistentTypes(Class<? extends Mapper> firstMapperClass,
                                     Map.Entry<Class, Class> firstMapperClassOutputTypes,
                                     Class<? extends Mapper> secondMapperClass) {
    Map.Entry<Class, Class> mapperOutputKeyValueTypes = getMapperOutputKeyValueTypes(secondMapperClass);
    if (!firstMapperClassOutputTypes.getKey().equals(mapperOutputKeyValueTypes.getKey())
      || !firstMapperClassOutputTypes.getValue().equals(mapperOutputKeyValueTypes.getValue())) {
      throw new IllegalArgumentException(
        String.format("Type mismatch in output type of mappers: %s and %s. " +
                        "Map output key types: %s and %s. " +
                        "Map output value types: %s and %s.",
                      firstMapperClass, secondMapperClass,
                      firstMapperClassOutputTypes.getKey(), mapperOutputKeyValueTypes.getKey(),
                      firstMapperClassOutputTypes.getValue(), mapperOutputKeyValueTypes.getValue()));
    }
  }

  private Map.Entry<Class, Class> getMapperOutputKeyValueTypes(Class<? extends Mapper> mapperClass) {
    TypeToken<Mapper> firstType = resolveClass(mapperClass, Mapper.class);
    Type[] firstTypeArgs = ((ParameterizedType) firstType.getType()).getActualTypeArguments();
    return new AbstractMap.SimpleEntry<Class, Class>(TypeToken.of(firstTypeArgs[2]).getRawType(),
                                                     TypeToken.of(firstTypeArgs[3]).getRawType());
  }

  /**
   * Sets the configurations used for inputs.
   * Multiple mappers could be defined, so we first check that their output types are consistent.
   *
   * @return the TypeToken for one of the mappers (doesn't matter which one, since we check that all of their output
   * key/value types are consistent. Returns null if the mapper class was not configured directly on the job and the
   * job's mapper class is to be used.
   * @throws IllegalArgumentException if any of the configured mapper output types are inconsistent.
   */
  @Nullable
  private TypeToken<Mapper> setInputsIfNeeded(Job job) throws IOException, ClassNotFoundException {
    Class<? extends Mapper> jobMapperClass = job.getMapperClass();

    Class<? extends Mapper> firstMapperClass = null;
    Map.Entry<Class, Class> firstMapperOutputTypes = null;

    for (Map.Entry<String, MapperInput> mapperInputEntry : context.getMapperInputs().entrySet()) {
      MapperInput mapperInput = mapperInputEntry.getValue();
      InputFormatProvider provider = mapperInput.getInputFormatProvider();
      Map<String, String> inputFormatConfiguration = mapperInput.getInputFormatConfiguration();

      // default to what is configured on the job, if user didn't specify a mapper for an input
      Class<? extends Mapper> mapperClass = mapperInput.getMapper() == null ? jobMapperClass : mapperInput.getMapper();

      // check output key/value type consistency, except for the first input
      if (firstMapperClass == null) {
        firstMapperClass = mapperClass;
        firstMapperOutputTypes = getMapperOutputKeyValueTypes(mapperClass);
      } else {
        assertConsistentTypes(firstMapperClass, firstMapperOutputTypes, mapperClass);
      }

      MultipleInputs.addInput(job, mapperInputEntry.getKey(),
                              mapperInput.getInputFormatClassName(), inputFormatConfiguration, mapperClass);
    }

    // if firstMapperClass is null, then, user is not going through our APIs to add input; leave the job's input format
    // to user and simply return the mapper output types of the mapper configured on the job.
    // if firstMapperClass == jobMapperClass, return null if the user didn't configure the mapper class explicitly
    if (firstMapperClass == null || firstMapperClass == jobMapperClass) {
      return resolveClass(job.getConfiguration(), MRJobConfig.MAP_CLASS_ATTR, Mapper.class);
    }
    return resolveClass(firstMapperClass, Mapper.class);
  }

  /**
   * Sets the configurations used for outputs.
   */
  private void setOutputsIfNeeded(Job job) throws ClassNotFoundException {
    List<ProvidedOutput> outputsMap = context.getOutputs();
    fixOutputPermissions(job, outputsMap);
    LOG.debug("Using as output for MapReduce Job: {}", outputsMap);
    MultipleOutputsMainOutputWrapper.setOutputs(job, outputsMap);
    job.setOutputFormatClass(MultipleOutputsMainOutputWrapper.class);
  }

  private void fixOutputPermissions(JobContext job, List<ProvidedOutput> outputs) {
    Configuration jobconf = job.getConfiguration();
    Set<String> outputsWithUmask = new HashSet<>();
    Set<String> outputUmasks = new HashSet<>();
    for (ProvidedOutput entry : outputs) {
      String umask = entry.getOutputFormatConfiguration().get(HADOOP_UMASK_PROPERTY);
      if (umask != null) {
        outputsWithUmask.add(entry.getOutput().getAlias());
        outputUmasks.add(umask);
      }
    }
    boolean allOutputsHaveUmask = outputsWithUmask.size() == outputs.size();
    boolean allOutputsAgree = outputUmasks.size() == 1;
    boolean jobConfHasUmask = isProgrammaticConfig(jobconf, HADOOP_UMASK_PROPERTY);
    String jobConfUmask = jobconf.get(HADOOP_UMASK_PROPERTY);

    boolean mustFixUmasks = false;
    if (jobConfHasUmask) {
      // case 1: job conf has a programmatic umask. It prevails.
      mustFixUmasks = !outputsWithUmask.isEmpty();
      if (mustFixUmasks) {
        LOG.info("Overriding permissions of outputs {} because a umask of {} was set programmatically in the job " +
                   "configuration.", outputsWithUmask, jobConfUmask);
      }
    } else if (allOutputsHaveUmask && allOutputsAgree) {
      // case 2: no programmatic umask in job conf, all outputs want the same umask: set it in job conf
      String umaskToUse = outputUmasks.iterator().next();
      jobconf.set(HADOOP_UMASK_PROPERTY, umaskToUse);
      LOG.debug("Setting umask of {} in job configuration because all outputs {} agree on it.",
                umaskToUse, outputsWithUmask);
    } else {
      // case 3: some outputs configure a umask, but not all of them, or not all the same: use job conf default
      mustFixUmasks = !outputsWithUmask.isEmpty();
      if (mustFixUmasks) {
        LOG.warn("Overriding permissions of outputs {} because they configure different permissions. Falling back " +
                   "to default umask of {} in job configuration.", outputsWithUmask, jobConfUmask);
      }
    }

    // fix all output configurations that have a umask by removing that property from their configs
    if (mustFixUmasks) {
      for (int i = 0; i < outputs.size(); i++) {
        ProvidedOutput output = outputs.get(i);
        if (outputsWithUmask.contains(output.getOutput().getAlias())) {
          Map<String, String> outputConfig = new HashMap<>(output.getOutputFormatConfiguration());
          outputConfig.remove(HADOOP_UMASK_PROPERTY);
          outputs.set(i, new ProvidedOutput(output.getOutput(), output.getOutputFormatProvider(),
                                            output.getOutputFormatClassName(), outputConfig));
        }
      }
    }
  }

  private String getJobName(BasicMapReduceContext context) {
    ProgramId programId = context.getProgram().getId();
    // MRJobClient expects the following format (for RunId to be the first component)
    return String.format("%s.%s.%s.%s.%s",
                         context.getRunId().getId(), ProgramType.MAPREDUCE.name().toLowerCase(),
                         programId.getNamespace(), programId.getApplication(), programId.getProgram());
  }

  /**
   * Creates a jar that contains everything that are needed for running the MapReduce program by Hadoop.
   *
   * @return a new {@link File} containing the job jar
   */
  private File buildJobJar(Job job, File tempDir) throws IOException, URISyntaxException {
    File jobJar = new File(tempDir, "job.jar");
    LOG.debug("Creating Job jar: {}", jobJar);

    // For local mode, nothing is needed in the job jar since we use the classloader in the configuration object.
    if (MapReduceTaskContextProvider.isLocal(job.getConfiguration())) {
      JarOutputStream output = new JarOutputStream(new FileOutputStream(jobJar));
      output.close();
      return jobJar;
    }

    // Excludes libraries that are for sure not needed.
    // Hadoop - Available from the cluster
    // Spark - MR never uses Spark
    ClassAcceptor hadoopClassExcluder = new ProgramRuntimeClassAcceptor();
    ApplicationBundler appBundler = new ApplicationBundler(new ClassAcceptor() {
      @Override
      public boolean accept(String className, URL classUrl, URL classPathUrl) {
        if (className.startsWith("org.apache.spark") || classPathUrl.toString().contains("spark-assembly")) {
          return false;
        }
        return hadoopClassExcluder.accept(className, classUrl, classPathUrl);
      }
    });
    Set<Class<?>> classes = Sets.newHashSet();
    classes.add(MapReduce.class);
    classes.add(MapperWrapper.class);
    classes.add(ReducerWrapper.class);
    classes.add(SLF4JBridgeHandler.class);
    classes.add(MapReduceContainerLauncher.class);

    // We only need to trace the Input/OutputFormat class due to MAPREDUCE-5957 so that those classes are included
    // in the job.jar and be available in the MR system classpath before our job classloader (ApplicationClassLoader)
    // take over the classloading.
    if (cConf.getBoolean(Constants.AppFabric.MAPREDUCE_INCLUDE_CUSTOM_CLASSES)) {
      try {
        Class<? extends InputFormat<?, ?>> inputFormatClass = job.getInputFormatClass();
        classes.add(inputFormatClass);
      } catch (Throwable t) {
        LOG.debug("InputFormat class not found: {}", t.getMessage(), t);
        // Ignore
      }
      try {
        Class<? extends OutputFormat<?, ?>> outputFormatClass = job.getOutputFormatClass();
        classes.add(outputFormatClass);
      } catch (Throwable t) {
        LOG.debug("OutputFormat class not found: {}", t.getMessage(), t);
        // Ignore
      }
    }
    // End of MAPREDUCE-5957.

    // Add KMS class
    if (SecureStoreUtils.isKMSBacked(cConf) && SecureStoreUtils.isKMSCapable()) {
      classes.add(SecureStoreUtils.getKMSSecureStore());
    }

    if (clusterMode == ClusterMode.ON_PREMISE) {
      try {
        Class<?> hbaseTableUtilClass = HBaseTableUtilFactory.getHBaseTableUtilClass(cConf);
        classes.add(hbaseTableUtilClass);
      } catch (ProvisionException e) {
        LOG.warn("Not including HBaseTableUtil classes in submitted Job Jar since they are not available");
      }
    }

    ClassLoader oldCLassLoader = ClassLoaders.setContextClassLoader(getClass().getClassLoader());

    try {
      appBundler.createBundle(Locations.toLocation(jobJar), classes);
    } finally {
      ClassLoaders.setContextClassLoader(oldCLassLoader);
    }

    LOG.debug("Built MapReduce Job Jar at {}", jobJar.toURI());
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
  @VisibleForTesting
  @Nullable
  static <V> TypeToken<V> resolveClass(Configuration conf, String typeAttr, Class<V> superType) {
    Class<? extends V> userClass = conf.getClass(typeAttr, null, superType);
    if (userClass == null) {
      return null;
    }
    return resolveClass(userClass, superType);
  }

  /**
   * Returns a resolved {@link TypeToken} of the given super type of the class.
   *
   * @param userClass the user class of which we want the TypeToken
   * @param superType Super type of the class
   * @param <V> Type of the super type
   * @return A resolved {@link TypeToken}
   */
  @SuppressWarnings("unchecked")
  private static <V> TypeToken<V> resolveClass(Class<? extends V> userClass, Class<V> superType) {
    return (TypeToken<V>) TypeToken.of(userClass).getSupertype(superType);
  }

  /**
   * Sets the output key and value classes in the job configuration by inspecting the {@link Mapper} and {@link Reducer}
   * if it is not set by the user.
   *
   * @param job the MapReduce job
   * @param mapperTypeToken TypeToken of a configured mapper (may not be configured on the job). Has already been
   *                        resolved from the job's mapper class.
   */
  private void setOutputClassesIfNeeded(Job job, @Nullable TypeToken<?> mapperTypeToken) {
    Configuration conf = job.getConfiguration();

    // Try to get the type from reducer
    TypeToken<?> type = resolveClass(conf, MRJobConfig.REDUCE_CLASS_ATTR, Reducer.class);

    if (type == null) {
      // Map only job
      type = mapperTypeToken;
    }

    // If not able to detect type, nothing to set
    if (type == null || !(type.getType() instanceof ParameterizedType)) {
      return;
    }

    Type[] typeArgs = ((ParameterizedType) type.getType()).getActualTypeArguments();

    // Set it only if the user didn't set it in initialize
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
   * @param mapperTypeToken TypeToken of a configured mapper (may not be configured on the job). Has already been
   *                        resolved from the job's mapper class.
   */
  private void setMapOutputClassesIfNeeded(Job job, @Nullable TypeToken<?> mapperTypeToken) {
    Configuration conf = job.getConfiguration();

    TypeToken<?> type = mapperTypeToken;
    int keyIdx = 2;
    int valueIdx = 3;

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

    // Set it only if the user didn't set it in initialize
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

  // Regex pattern for configuration source if it is set programmatically. This constant is not defined in Hadoop
  // Hadoop 2.3.0 and before has a typo as 'programatically', while it is fixed later as 'programmatically'.
  private static final Pattern PROGRAMATIC_SOURCE_PATTERN = Pattern.compile("program{1,2}atically");

  /**
   * Returns, based on the sources of a configuration property, whether this property was set programmatically.
   * This is the case if the first (that is, oldest) source is "programatic" or "programmatic" (depending on the
   * Hadoop version).
   *
   * Note: the "program(m)atic" will always be the first source. If the configuration is written to a file and
   * then loaded again, then the file name will become the second source, etc. This is the case, for example,
   * in a mapper or reducer task.
   *
   * See {@link Configuration#getPropertySources(String)}.
   */
  private static boolean isProgrammaticConfig(Configuration conf, String name) {
    String[] sources = conf.getPropertySources(name);
    return sources != null && sources.length > 0 && PROGRAMATIC_SOURCE_PATTERN.matcher(sources[0]).matches();
  }


  /**
   * Copies a plugin archive jar to the target location.
   *
   * @param targetDir directory where the archive jar should be created
   * @return {@link Location} to the plugin archive or {@code null} if no plugin archive is available from the context.
   */
  @Nullable
  private Location createPluginArchive(Location targetDir) throws IOException {
    File pluginArchive = context.getPluginArchive();
    if (pluginArchive == null) {
      return null;
    }
    return copyFileToLocation(pluginArchive, targetDir);
  }

  /**
   * Copies a file to the target location.
   *
   * @param targetDir directory where the file should be copied to.
   * @return {@link Location} to the file or {@code null} if given file is {@code null}.
   */
  private Location copyFileToLocation(File file, Location targetDir) throws IOException {
    Location targetLocation = targetDir.append(file.getName()).getTempFile(".jar");
    Files.copy(file, Locations.newOutputSupplier(targetLocation));
    return targetLocation;
  }

  /**
   * Creates a temp copy of the program jar.
   *
   * @return a new {@link Location} which contains the same content as the program jar
   */
  private Location copyProgramJar(Location targetDir) throws IOException {
    Location programJarCopy = targetDir.append("program.jar");

    ByteStreams.copy(Locations.newInputSupplier(programJarLocation), Locations.newOutputSupplier(programJarCopy));
    LOG.debug("Copied Program Jar to {}, source: {}", programJarCopy, programJarLocation);
    return programJarCopy;
  }

  /**
   * Creates a launcher jar that contains the MR AM main class and the MR task main class. It is for ClassLoader
   * construction before delegating the actual execution to the actual MR main classes.
   *
   * @see MapReduceContainerLauncher
   * @see ContainerLauncherGenerator
   */
  private Location createLauncherJar(Location targetDir) throws IOException {
    Location launcherJar = targetDir.append("launcher.jar");

    ContainerLauncherGenerator.generateLauncherJar(
      Arrays.asList(
        "org.apache.hadoop.mapreduce.v2.app.MRAppMaster",
        "org.apache.hadoop.mapred.YarnChild"
      ), MapReduceContainerLauncher.class, launcherJar);
    return launcherJar;
  }

  private Runnable createCleanupTask(final Object...resources) {
    return () -> {
      for (Object resource : resources) {
        if (resource == null) {
          continue;
        }

        try {
          if (resource instanceof File) {
            if (((File) resource).isDirectory()) {
              DirUtils.deleteDirectoryContents((File) resource);
            } else {
              ((File) resource).delete();
            }
          } else if (resource instanceof Location) {
            Locations.deleteQuietly((Location) resource, true);
          } else if (resource instanceof AutoCloseable) {
            ((AutoCloseable) resource).close();
          } else if (resource instanceof Runnable) {
            ((Runnable) resource).run();
          }
        } catch (Throwable t) {
          LOG.warn("Exception when cleaning up resource {}", resource, t);
        }
      }
    };
  }

  @VisibleForTesting
  enum TaskType {
    MAP(Job.MAP_MEMORY_MB, Job.DEFAULT_MAP_MEMORY_MB, Job.MAP_JAVA_OPTS),
    REDUCE(Job.REDUCE_MEMORY_MB, Job.DEFAULT_REDUCE_MEMORY_MB, Job.REDUCE_JAVA_OPTS);

    private final String memoryConfKey;
    private final int defaultMemoryMB;
    private final String javaOptsKey;
    private final String vcoreConfKey;
    private final int defaultVcores;

    TaskType(String memoryConfKey, int defaultMemoryMB, String javaOptsKey) {
      this.memoryConfKey = memoryConfKey;
      this.defaultMemoryMB = defaultMemoryMB;
      this.javaOptsKey = javaOptsKey;

      String vcoreConfKey = null;
      int defaultVcores = 0;
      try {
        String confFieldName = name() + "_CPU_VCORES";
        vcoreConfKey = Job.class.getField(confFieldName).get(null).toString();

        String defaultValueFieldName = "DEFAULT_" + confFieldName;
        defaultVcores = (Integer) Job.class.getField(defaultValueFieldName).get(null);

      } catch (Exception e) {
        // OK to ignore
        // Some older version of hadoop-mr-client doesn't has the VCORES field as vcores was not supported in YARN.
      }
      this.vcoreConfKey = vcoreConfKey;
      this.defaultVcores = defaultVcores;
    }

    /**
     * Sets up the configuration for the task represented by this type.
     *
     * @param conf configuration to modify
     * @param cConf the cdap configuration
     * @param runtimeArgs the runtime arguments applicable for this task type
     * @param resources resources information or {@code null} if resources is not set in the
     *                  {@link MapReduceSpecification} during deployment time and also not set through
     *                  runtime arguments with the {@link SystemArguments#MEMORY_KEY} setting.
     */
    public void configure(Configuration conf, CConfiguration cConf,
                          Map<String, String> runtimeArgs, @Nullable Resources resources) {

      // Sets the container vcores if it is not set programmatically by the initialize() method, but is provided
      // through runtime arguments or MR spec.
      int vcores = 0;
      String vcoreSource = null;
      if (vcoreConfKey != null) {
        if (isProgrammaticConfig(conf, vcoreConfKey) || resources == null) {
          vcores = conf.getInt(vcoreConfKey, defaultVcores);
          vcoreSource = "from job configuration";
        } else {
          vcores = resources.getVirtualCores();
          conf.setInt(vcoreConfKey, vcores);
          vcoreSource = runtimeArgs.containsKey(SystemArguments.CORES_KEY)
            ? "from runtime arguments " + SystemArguments.CORES_KEY : "programmatically";
        }
      }

      int memoryMB;
      String memorySource;
      if (isProgrammaticConfig(conf, memoryConfKey)) {
        // If set programmatically, get the memory size from the config.
        memoryMB = conf.getInt(memoryConfKey, defaultMemoryMB);
        memorySource = "from job configuration";
      } else {
        // Sets the container memory size if it is not set programmatically by the initialize() method.
        memoryMB = resources == null ? conf.getInt(memoryConfKey, defaultMemoryMB) : resources.getMemoryMB();
        conf.setInt(memoryConfKey, memoryMB);
        memorySource = runtimeArgs.containsKey(SystemArguments.MEMORY_KEY)
          ? "from runtime arguments " + SystemArguments.MEMORY_KEY : "programmatically";
      }

      // Setup the jvm opts, which includes options from cConf and the -Xmx setting
      // Get the default settings from cConf
      int reservedMemory = cConf.getInt(Configs.Keys.JAVA_RESERVED_MEMORY_MB);
      double minHeapRatio = cConf.getDouble(Configs.Keys.HEAP_RESERVED_MIN_RATIO);
      String maxHeapSource =
        "from system configuration " + Configs.Keys.JAVA_RESERVED_MEMORY_MB +
          " and " + Configs.Keys.HEAP_RESERVED_MIN_RATIO;

      // Optionally overrides the settings from the runtime arguments
      Map<String, String> configs = SystemArguments.getTwillContainerConfigs(runtimeArgs, memoryMB);
      if (configs.containsKey(Configs.Keys.JAVA_RESERVED_MEMORY_MB)) {
        reservedMemory = Integer.parseInt(configs.get(Configs.Keys.JAVA_RESERVED_MEMORY_MB));
        maxHeapSource = "from runtime arguments " + SystemArguments.RESERVED_MEMORY_KEY_OVERRIDE;
      }
      if (configs.containsKey(Configs.Keys.HEAP_RESERVED_MIN_RATIO)) {
        minHeapRatio = Double.parseDouble(configs.get(Configs.Keys.HEAP_RESERVED_MIN_RATIO));
      }

      int maxHeapSize = org.apache.twill.internal.utils.Resources.computeMaxHeapSize(memoryMB,
                                                                                     reservedMemory, minHeapRatio);

      String jvmOpts = cConf.get(Constants.AppFabric.PROGRAM_JVM_OPTS) + " -Xmx" + maxHeapSize + "m";
      // If the job conf has the jvm opts set programmatically, then prepend it with what being determined above;
      // otherwise, append. The reason is, we want to have the one set programmatically has higher precedence,
      // while the one set cluster wide (in mapred-site.xml) has lower precedence.
      jvmOpts = isProgrammaticConfig(conf, javaOptsKey)
        ? jvmOpts + " " + conf.get(javaOptsKey, "")
        : conf.get(javaOptsKey, "") + " " + jvmOpts;
      conf.set(javaOptsKey, jvmOpts.trim());

      LOG.debug("MapReduce {} task container memory is {}MB, set {}. " +
                  "Maximum heap memory size of {}MB, set {}.{}",
                name().toLowerCase(), memoryMB, memorySource, maxHeapSize, maxHeapSource,
                vcores > 0 ? " Virtual cores is " + vcores + ", set " + vcoreSource + "." : ".");
    }
  }

  /**
   * Localizes resources requested by users in the MapReduce Program's beforeSubmit phase.
   * In Local mode, also copies resources to a temporary directory.
   *
   * @param job the {@link Job} for this MapReduce program
   * @param targetDir in local mode, a temporary directory to copy the resources to
   * @return a {@link Map} of resource name to the resource path. The resource path will be absolute in local mode,
   * while it will just contain the file name in distributed mode.
   */
  private Map<String, String> localizeUserResources(Job job, File targetDir) throws IOException {
    Map<String, String> localizedResources = new HashMap<>();
    Map<String, LocalizeResource> resourcesToLocalize = context.getResourcesToLocalize();
    for (Map.Entry<String, LocalizeResource> entry : resourcesToLocalize.entrySet()) {
      String localizedFilePath;
      String name = entry.getKey();
      Configuration mapredConf = job.getConfiguration();
      if (MapReduceTaskContextProvider.isLocal(mapredConf)) {
        // in local mode, also add localize resources in a temporary directory
        localizedFilePath =
          LocalizationUtils.localizeResource(entry.getKey(), entry.getValue(), targetDir).getAbsolutePath();
      } else {
        URI uri = entry.getValue().getURI();
        // in distributed mode, use the MapReduce Job object to localize resources
        URI actualURI;
        try {
          actualURI = new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), uri.getQuery(), name);
        } catch (URISyntaxException e) {
          // Most of the URI is constructed from the passed URI. So ideally, this should not happen.
          // If it does though, there is nothing that clients can do to recover, so not propagating a checked exception.
          throw Throwables.propagate(e);
        }
        if (entry.getValue().isArchive()) {
          job.addCacheArchive(actualURI);
        } else {
          job.addCacheFile(actualURI);
        }
        localizedFilePath = name;
      }
      LOG.debug("MapReduce Localizing file {} {}", entry.getKey(), entry.getValue());
      localizedResources.put(name, localizedFilePath);
    }
    return localizedResources;
  }

  /**
   * Prepares the {@link HBaseDDLExecutor} implementation for localization.
   */
  @Nullable
  private String getLocalizedHBaseDDLExecutorDir(File tempDir, CConfiguration cConf, Job job,
                                                 Location tempLocation) throws IOException {
    String ddlExecutorExtensionDir = cConf.get(Constants.HBaseDDLExecutor.EXTENSIONS_DIR);
    if (ddlExecutorExtensionDir == null) {
      // Nothing to localize
      return null;
    }

    String hbaseDDLExtensionJarName = "hbaseddlext.jar";
    final File target = new File(tempDir, hbaseDDLExtensionJarName);
    BundleJarUtil.createJar(new File(ddlExecutorExtensionDir), target);
    Location targetLocation = tempLocation.append(hbaseDDLExtensionJarName);
    Files.copy(target, Locations.newOutputSupplier(targetLocation));
    job.addCacheArchive(targetLocation.toURI());
    return target.getName();
  }
}
