/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.ProgramState;
import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkClientContext;
import co.cask.cdap.app.runtime.spark.distributed.SparkContainerLauncher;
import co.cask.cdap.app.runtime.spark.submit.SparkSubmitter;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.CConfigurationUtil;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.PropertyFieldSetter;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.runtime.DataSetFieldSetter;
import co.cask.cdap.internal.app.runtime.LocalizationUtils;
import co.cask.cdap.internal.app.runtime.MetricsFieldSetter;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.batch.distributed.ContainerLauncherGenerator;
import co.cask.cdap.internal.app.runtime.distributed.LocalizeResource;
import co.cask.cdap.internal.lang.Fields;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.common.internal.io.UnsupportedTypeException;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.SparkSubmit;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.zip.Deflater;
import javax.annotation.Nullable;

/**
 * Performs the actual execution of Spark job.
 * <p/>
 * Service start -> Performs job setup, and initialize call.
 * Service run -> Submits the spark job through {@link SparkSubmit}
 * Service triggerStop -> kill job
 * Service stop -> Commit/invalidate transaction, destroy, cleanup
 */
final class SparkRuntimeService extends AbstractExecutionThreadService {

  private static final String CDAP_LAUNCHER_JAR = "cdap-spark-launcher.jar";
  private static final String CDAP_SPARK_JAR = "cdap-spark.jar";
  private static final String CDAP_METRICS_PROPERTIES = "metrics.properties";

  private static final Logger LOG = LoggerFactory.getLogger(SparkRuntimeService.class);

  private final CConfiguration cConf;
  private final Spark spark;
  private final SparkRuntimeContext runtimeContext;
  private final File pluginArchive;
  private final SparkSubmitter sparkSubmitter;
  private final AtomicReference<ListenableFuture<RunId>> completion;
  private final BasicSparkClientContext context;

  private Callable<ListenableFuture<RunId>> submitSpark;
  private Runnable cleanupTask;

  SparkRuntimeService(CConfiguration cConf, Spark spark, @Nullable File pluginArchive,
                      SparkRuntimeContext runtimeContext, SparkSubmitter sparkSubmitter) {
    this.cConf = cConf;
    this.spark = spark;
    this.runtimeContext = runtimeContext;
    this.pluginArchive = pluginArchive;
    this.sparkSubmitter = sparkSubmitter;
    this.completion = new AtomicReference<>();
    this.context = new BasicSparkClientContext(runtimeContext);
  }

  @Override
  protected String getServiceName() {
    return "Spark - " + runtimeContext.getSparkSpecification().getName();
  }

  @Override
  protected void startUp() throws Exception {
    // additional spark job initialization at run-time
    // This context is for calling initialize and onFinish on the Spark program

    // Fields injection for the Spark program
    // It has to be done in here instead of in SparkProgramRunner for the @UseDataset injection
    // since the dataset cache being used in Spark is a MultiThreadDatasetCache
    // The AbstractExecutionThreadService guarantees that startUp(), run() and shutDown() all happens in the same thread
    Reflections.visit(spark, spark.getClass(),
                      new PropertyFieldSetter(runtimeContext.getSparkSpecification().getProperties()),
                      new DataSetFieldSetter(runtimeContext.getDatasetCache()),
                      new MetricsFieldSetter(runtimeContext));

    // Since we are updating cConf, make a copy of it to which updates will be made.
    CConfiguration cConfCopy = CConfiguration.copy(cConf);
    // Creates a temporary directory locally for storing all generated files.
    File tempDir = DirUtils.createTempDir(new File(cConfCopy.get(Constants.CFG_LOCAL_DATA_DIR),
                                                   cConfCopy.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile());
    tempDir.mkdirs();
    this.cleanupTask = createCleanupTask(tempDir, System.getProperties());
    try {
      initialize();
      SparkRuntimeContextConfig contextConfig = new SparkRuntimeContextConfig(runtimeContext.getConfiguration());

      final File jobJar = generateJobJar(tempDir, contextConfig.isLocal(), cConfCopy);
      final List<LocalizeResource> localizeResources = new ArrayList<>();

      String metricsConfPath;
      String classpath = "";

      // Setup the SparkConf with properties from spark-defaults.conf
      Properties sparkDefaultConf = SparkPackageUtils.getSparkDefaultConf();
      for (String key : sparkDefaultConf.stringPropertyNames()) {
        SparkRuntimeEnv.setProperty(key, sparkDefaultConf.getProperty(key));
      }

      if (contextConfig.isLocal()) {
        // In local mode, always copy (or link if local) user requested resources
        copyUserResources(context.getLocalizeResources(), tempDir);

        File metricsConf = SparkMetricsSink.writeConfig(new File(tempDir, CDAP_METRICS_PROPERTIES));
        metricsConfPath = metricsConf.getAbsolutePath();
      } else {
        // Localize all user requested files in distributed mode
        distributedUserResources(context.getLocalizeResources(), localizeResources);

        // Localize program jar and the expanding program jar
        File programJar = Locations.linkOrCopy(runtimeContext.getProgram().getJarLocation(),
                                               new File(tempDir, SparkRuntimeContextProvider.PROGRAM_JAR_NAME));
        File expandedProgramJar = Locations.linkOrCopy(runtimeContext.getProgram().getJarLocation(),
                                                       new File(tempDir,
                                                                SparkRuntimeContextProvider.PROGRAM_JAR_EXPANDED_NAME));
        // Localize both the unexpanded and expanded program jar
        localizeResources.add(new LocalizeResource(programJar));
        localizeResources.add(new LocalizeResource(expandedProgramJar, true));

        if (pluginArchive != null) {
          localizeResources.add(new LocalizeResource(pluginArchive, true));
        }

        // Create and localize the launcher jar, which is for setting up services and classloader for spark containers
        localizeResources.add(new LocalizeResource(createLauncherJar(tempDir)));

        // Create metrics conf file in the current directory since
        // the same value for the "spark.metrics.conf" config needs to be used for both driver and executor processes
        // Also localize the metrics conf file to the executor nodes
        File metricsConf = SparkMetricsSink.writeConfig(new File(CDAP_METRICS_PROPERTIES));
        metricsConfPath = metricsConf.getName();
        localizeResources.add(new LocalizeResource(metricsConf));

        prepareHBaseDDLExecutorResources(tempDir, cConfCopy, localizeResources);

        // Localize the cConf file
        localizeResources.add(new LocalizeResource(saveCConf(cConfCopy, tempDir)));

        // Preserves and localize runtime information in the hConf
        Configuration hConf = contextConfig.set(runtimeContext, pluginArchive).getConfiguration();
        localizeResources.add(new LocalizeResource(saveHConf(hConf, tempDir)));

        // Joiner for creating classpath for spark containers
        Joiner joiner = Joiner.on(File.pathSeparator).skipNulls();

        // Localize the spark.jar archive, which contains all CDAP and dependency jars
        File sparkJar = new File(tempDir, CDAP_SPARK_JAR);
        classpath = joiner.join(Iterables.transform(buildDependencyJar(sparkJar), new Function<String, String>() {
          @Override
          public String apply(String name) {
            return Paths.get("$PWD", CDAP_SPARK_JAR, name).toString();
          }
        }));
        localizeResources.add(new LocalizeResource(sparkJar, true));

        // Localize logback if there is one. It is placed at the beginning of the classpath
        File logbackJar = ProgramRunners.createLogbackJar(new File(tempDir, "logback.xml.jar"));
        if (logbackJar != null) {
          localizeResources.add(new LocalizeResource(logbackJar));
          classpath = joiner.join(Paths.get("$PWD", logbackJar.getName()), classpath);
        }

        // Localize extra jars and append to the end of the classpath
        List<String> extraJars = new ArrayList<>();
        for (URI jarURI : CConfigurationUtil.getExtraJars(cConfCopy)) {
          extraJars.add(Paths.get("$PWD", LocalizationUtils.getLocalizedName(jarURI)).toString());
          localizeResources.add(new LocalizeResource(jarURI, false));
        }
        classpath = joiner.join(classpath, joiner.join(extraJars));
      }

      final Map<String, String> configs = createSubmitConfigs(tempDir, metricsConfPath, classpath,
                                                              context.getLocalizeResources(),
                                                              contextConfig.isLocal());
      submitSpark = new Callable<ListenableFuture<RunId>>() {
        @Override
        public ListenableFuture<RunId> call() throws Exception {
          // If stop already requested on this service, don't submit the spark.
          // This happen when stop() was called whiling starting
          if (!isRunning()) {
            return immediateCancelledFuture();
          }
          return sparkSubmitter.submit(runtimeContext, configs, localizeResources, jobJar, runtimeContext.getRunId());
        }
      };
    } catch (LinkageError e) {
      // Need to wrap LinkageError. Otherwise, listeners of this Guava Service may not be called if the initialization
      // of the user program is missing dependencies (CDAP-2543)
      throw new Exception(e.getMessage(), e);
    } catch (Throwable t) {
      cleanupTask.run();
      throw t;
    }
  }

  @Override
  protected void run() throws Exception {
    ListenableFuture<RunId> jobCompletion = completion.getAndSet(submitSpark.call());
    // If the jobCompletion is not null, meaning the stop() was called before the atomic reference "completion" has been
    // updated. This mean the job is cancelled. We also need to cancel the future returned by submitSpark.call().
    if (jobCompletion != null) {
      completion.get().cancel(true);
    } else {
      // It's possible that the completion reference is changed by the triggerShutdown call between the getAndSet()
      // and the get() call here. But it's ok since the triggeredShutdown will always put a cancelled future in the
      // atomic reference and cancel the actual one.
      jobCompletion = completion.get();
    }

    try {
      // Block for job completion
      jobCompletion.get();
    } catch (Exception e) {
      // See if it is due to job cancelation. If it is, then it's not an error.
      if (jobCompletion.isCancelled()) {
        LOG.info("Spark program execution cancelled: {}", runtimeContext);
      } else {
        throw e;
      }
    }
  }

  @Override
  protected void shutDown() throws Exception {
    // Try to get from the submission future to see if the job completed successfully.
    ListenableFuture<RunId> jobCompletion = completion.get();
    ProgramState state = new ProgramState(ProgramStatus.COMPLETED, null);
    try {
      jobCompletion.get();
    } catch (Exception e) {
      if (jobCompletion.isCancelled()) {
        state = new ProgramState(ProgramStatus.KILLED, null);
      } else {
        state = new ProgramState(ProgramStatus.FAILED, Throwables.getRootCause(e).getMessage());
      }
    }

    try {
      destroy(state);
    } finally {
      cleanupTask.run();
      LOG.debug("Spark program completed: {}", runtimeContext);
    }
  }

  @Override
  protected void triggerShutdown() {
    LOG.debug("Stop requested for Spark Program {}", runtimeContext);
    // Replace the completion future with a cancelled one.
    // Also, try to cancel the current completion future if it exists.
    ListenableFuture<RunId> future = completion.getAndSet(this.<RunId>immediateCancelledFuture());
    if (future != null) {
      future.cancel(true);
    }
  }

  @Override
  protected Executor executor() {
    // Always execute in new daemon thread.
    //noinspection NullableProblems
    return new Executor() {
      @Override
      public void execute(final Runnable runnable) {
        final Thread t = new Thread(new Runnable() {

          @Override
          public void run() {
            // note: this sets logging context on the thread level
            LoggingContextAccessor.setLoggingContext(runtimeContext.getLoggingContext());
            runnable.run();
          }
        });
        t.setDaemon(true);
        t.setName("SparkRunner" + runtimeContext.getProgramName());
        t.start();
      }
    };
  }

  /**
   * Calls the {@link Spark#beforeSubmit(SparkClientContext)} for the pre 3.5 Spark programs, calls
   * the {@link ProgramLifecycle#initialize} otherwise.
   */
  @SuppressWarnings("unchecked")
  private void initialize() throws Exception {
    // AbstractSpark implements final initialize(context) and requires subclass to
    // implement initialize(), whereas programs that directly implement Spark have
    // the option to override initialize(context) (if they implement ProgramLifeCycle)
    final TransactionControl txControl = spark instanceof AbstractSpark
      ? Transactions.getTransactionControl(TransactionControl.IMPLICIT, AbstractSpark.class, spark, "initialize")
      : spark instanceof ProgramLifecycle
      ? Transactions.getTransactionControl(TransactionControl.IMPLICIT, Spark.class,
                                           spark, "initialize", SparkClientContext.class)
      : TransactionControl.IMPLICIT;

    TxRunnable runnable = new TxRunnable() {
      @Override
      public void run(DatasetContext ctxt) throws Exception {
        Cancellable cancellable = SparkRuntimeUtils.setContextClassLoader(new SparkClassLoader(runtimeContext));
        try {
          context.setState(new ProgramState(ProgramStatus.INITIALIZING, null));
          if (spark instanceof ProgramLifecycle) {
            ((ProgramLifecycle) spark).initialize(context);
          } else {
            spark.beforeSubmit(context);
          }
        } finally {
          cancellable.cancel();
        }
      }
    };
    if (TransactionControl.IMPLICIT == txControl) {
      context.execute(runnable);
    } else {
      runnable.run(context);
    }
  }

  /**
   * Calls the destroy or onFinish method of {@link ProgramLifecycle}.
   */
  private void destroy(final ProgramState state) throws Exception {
    final TransactionControl txControl = spark instanceof ProgramLifecycle
      ? Transactions.getTransactionControl(TransactionControl.IMPLICIT, Spark.class, spark, "destroy")
      : TransactionControl.IMPLICIT;

    TxRunnable runnable = new TxRunnable() {
      @Override
      public void run(DatasetContext ctxt) throws Exception {
        Cancellable cancellable = SparkRuntimeUtils.setContextClassLoader(new SparkClassLoader(runtimeContext));
        try {
          context.setState(state);
          if (spark instanceof ProgramLifecycle) {
            ((ProgramLifecycle) spark).destroy();
          } else {
            spark.onFinish(state.getStatus() == ProgramStatus.COMPLETED, context);
          }
        } finally {
          cancellable.cancel();
        }
      }
    };
    if (TransactionControl.IMPLICIT == txControl) {
      context.execute(runnable);
    } else {
      runnable.run(context);
    }
  }

  /**
   * Creates a JAR file which contains generate Spark YARN container main classes. Those classes
   * are used for intercepting the Java main method in the YARN container so that we can control the
   * ClassLoader creation.
   */
  private File createLauncherJar(File tempDir) throws IOException {
    File jarFile = new File(tempDir, CDAP_LAUNCHER_JAR);
    ContainerLauncherGenerator.generateLauncherJar(
      Arrays.asList("org.apache.spark.deploy.yarn.ApplicationMaster",
                    "org.apache.spark.executor.CoarseGrainedExecutorBackend"),
      SparkContainerLauncher.class, Files.newOutputStreamSupplier(jarFile));
    return jarFile;
  }

  /**
   * Creates the configurations for the spark submitter.
   */
  private Map<String, String> createSubmitConfigs(File localDir, String metricsConfPath, String classpath,
                                                  Map<String, LocalizeResource> localizedResources,
                                                  boolean localMode) throws Exception {
    Map<String, String> configs = new HashMap<>();

    // Make Spark UI runs on random port. By default, Spark UI runs on port 4040 and it will do a sequential search
    // of the next port if 4040 is already occupied. However, during the process, it unnecessarily logs big stacktrace
    // as WARN, which pollute the logs a lot if there are concurrent Spark job running (e.g. a fork in Workflow).
    configs.put("spark.ui.port", "0");

    // Setup configs from the default spark conf
    Properties sparkDefaultConf = SparkPackageUtils.getSparkDefaultConf();
    configs.putAll(Maps.fromProperties(sparkDefaultConf));

    // Setup app.id and executor.id for Metric System
    configs.put("spark.app.id", context.getApplicationSpecification().getName());
    configs.put("spark.executor.id", context.getApplicationSpecification().getName());

    // Setups the resources requirements for driver and executor. The user can override it with the SparkConf.
    configs.put("spark.driver.memory", context.getDriverResources().getMemoryMB() + "m");
    configs.put("spark.driver.cores", String.valueOf(context.getDriverResources().getVirtualCores()));
    configs.put("spark.executor.memory", context.getExecutorResources().getMemoryMB() + "m");
    configs.put("spark.executor.cores", String.valueOf(context.getExecutorResources().getVirtualCores()));

    // Add user specified configs first. CDAP specifics config will override them later if there are duplicates.
    SparkConf sparkConf = context.getSparkConf();
    if (sparkConf != null) {
      for (Tuple2<String, String> tuple : sparkConf.getAll()) {
        configs.put(tuple._1(), tuple._2());
      }
    }

    // CDAP-5854: On Windows * is a reserved character which cannot be used in paths. So adding the below to
    // classpaths will fail. Please see CDAP-5854.
    // In local mode spark program runs under the same jvm as cdap master and these jars will already be in the
    // classpath so adding them is not required. In non-local mode where spark driver and executors runs in a different
    // jvm we are adding these to their classpath.
    if (!localMode) {
      String extraClassPath = Paths.get("$PWD", CDAP_LAUNCHER_JAR) + File.pathSeparator + classpath;

      // Set extraClasspath config by appending user specified extra classpath
      prependConfig(configs, "spark.driver.extraClassPath", extraClassPath, File.pathSeparator);
      prependConfig(configs, "spark.executor.extraClassPath", extraClassPath, File.pathSeparator);
    } else {
      // Only need to set this for local mode.
      // In distributed mode, Spark will not use this but instead use the yarn container directory.
      configs.put("spark.local.dir", localDir.getAbsolutePath());
    }

    configs.put("spark.metrics.conf", metricsConfPath);
    SparkRuntimeUtils.setLocalizedResources(localizedResources.keySet(), configs);

    return configs;
  }

  /**
   * Sets a key value pair to the given configuration map. If the key already exist, the given value will be
   * prepended to the existing value.
   */
  private void prependConfig(Map<String, String> configs, String key, String prepend, String separator) {
    String existing = configs.get(key);
    if (existing == null) {
      configs.put(key, prepend);
    } else {
      configs.put(key, prepend + separator + existing);
    }
  }

  /**
   * Packages all the dependencies of the Spark job. It contains all CDAP classes that are needed to run the
   * user spark program.
   *
   * @param targetFile the target file for the jar created
   * @return list of jar file name that contains all dependency jars in sorted order
   * @throws IOException if failed to package the jar
   */
  private Iterable<String> buildDependencyJar(File targetFile) throws IOException, URISyntaxException {
    Set<String> classpath = new TreeSet<>();
    try (JarOutputStream jarOut = new JarOutputStream(new BufferedOutputStream(new FileOutputStream(targetFile)))) {
      jarOut.setLevel(Deflater.NO_COMPRESSION);

      // Zip all the jar files under the same directory that contains the jar for this class and twill class.
      // Those are the directory created by TWILL that contains all dependency jars for this container
      for (String className : Arrays.asList(getClass().getName(), TwillRunnable.class.getName())) {
        Enumeration<URL> resources = getClass().getClassLoader().getResources(className.replace('.', '/') + ".class");
        while (resources.hasMoreElements()) {
          URL classURL = resources.nextElement();
          File libDir = new File(ClassLoaders.getClassPathURL(className, classURL).toURI()).getParentFile();

          for (File file : DirUtils.listFiles(libDir, "jar")) {
            if (classpath.add(file.getName())) {
              jarOut.putNextEntry(new JarEntry(file.getName()));
              Files.copy(file, jarOut);
              jarOut.closeEntry();
            }
          }
        }
      }
    }
    return classpath;
  }

  /**
   * Generates an empty JAR file.
   *
   * @return The generated {@link File} in the given target directory
   */
  private File generateJobJar(File tempDir, boolean isLocal, CConfiguration cConf) throws IOException {
    // in local mode, Spark Streaming with checkpointing will expect the same job jar to exist
    // in all runs of the program. This means it can't be created in the temporary directory for the run,
    // but must persist between runs
    File targetDir = isLocal ?
      new File(new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR), "runtime"), "spark") : tempDir;
    File tempFile = new File(targetDir, "emptyJob.jar");
    if (tempFile.exists()) {
      return tempFile;
    }
    DirUtils.mkdirs(targetDir.getAbsoluteFile());
    JarOutputStream output = new JarOutputStream(new FileOutputStream(tempFile));
    output.close();
    return tempFile;
  }

  /**
   * Serialize {@link CConfiguration} to a file.
   *
   * @return The {@link File} of the serialized configuration in the given target directory.
   */
  private File saveCConf(CConfiguration cConf, File targetDir) throws IOException {
    File file = new File(targetDir, SparkRuntimeContextProvider.CCONF_FILE_NAME);
    try (Writer writer = Files.newWriter(file, Charsets.UTF_8)) {
      cConf.writeXml(writer);
    }
    return file;
  }

  /**
   * Serialize {@link Configuration} to a file.
   *
   * @return The {@link File} of the serialized configuration in the given target directory.
   */
  private File saveHConf(Configuration hConf, File targetDir) throws IOException {
    File file = new File(targetDir, SparkRuntimeContextProvider.HCONF_FILE_NAME);
    try (Writer writer = Files.newWriter(file, Charsets.UTF_8)) {
      hConf.writeXml(writer);
    }
    return file;
  }

  /**
   * Localizes resources to the given local directory. For resources on the local directory already, a symlink will be
   * created. Otherwise, it will be copied.
   *
   * @param resources the set of resources that need to be localized.
   * @param targetDir the target directory for the resources to copy / link to.
   */
  private void copyUserResources(Map<String, LocalizeResource> resources, File targetDir) throws IOException {
    for (Map.Entry<String, LocalizeResource> entry : resources.entrySet()) {
      LocalizationUtils.localizeResource(entry.getKey(), entry.getValue(), targetDir);
    }
  }

  /**
   * Appends user localize resources to the given {@link List} of {@link LocalizeResource}.
   */
  private void distributedUserResources(Map<String, LocalizeResource> resources,
                                        List<LocalizeResource> result) throws URISyntaxException {
    for (Map.Entry<String, LocalizeResource> entry : resources.entrySet()) {
      URI uri = entry.getValue().getURI();
      URI actualURI = new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), uri.getQuery(), entry.getKey());
      result.add(new LocalizeResource(actualURI, entry.getValue().isArchive()));
    }
  }

  /**
   * Creates a {@link Runnable} to be executed to cleanup resources after executing the spark program.
   *
   * @param directory The directory to be deleted
   * @param properties The set of system {@link Properties} prior to the job execution.
   */
  private Runnable createCleanupTask(final File directory, Properties properties) {
    final Map<String, String> retainingProperties = new HashMap<>();
    for (String key : properties.stringPropertyNames()) {
      if (key.startsWith("spark.")) {
        retainingProperties.put(key, properties.getProperty(key));
      }
    }

    return new Runnable() {

      @Override
      public void run() {
        cleanupShutdownHooks();
        invalidateBeanIntrospectorCache();

        // Cleanup all system properties setup by SparkSubmit
        Iterable<String> sparkKeys = Iterables.filter(System.getProperties().stringPropertyNames(),
                                                      Predicates.containsPattern("^spark\\."));
        for (String key : sparkKeys) {
          if (retainingProperties.containsKey(key)) {
            String value = retainingProperties.get(key);
            LOG.debug("Restoring Spark system property: {} -> {}", key, value);
            System.setProperty(key, value);
          } else {
            LOG.debug("Removing Spark system property: {}", key);
            System.clearProperty(key);
          }
        }

        try {
          DirUtils.deleteDirectoryContents(directory);
        } catch (IOException e) {
          LOG.warn("Failed to cleanup directory {}", directory);
        }
      }
    };
  }

  /**
   * Cleanup all shutdown hooks added by Spark and execute them directly.
   * This is needed so that for CDAP standalone, it won't leak memory through shutdown hooks.
   */
  private void cleanupShutdownHooks() {
    // With Hadoop 2, Spark uses the Hadoop ShutdownHookManager
    ShutdownHookManager manager = ShutdownHookManager.get();
    try {
      // Use reflection to get the shutdown hooks
      Method getShutdownHooksInOrder = manager.getClass().getDeclaredMethod("getShutdownHooksInOrder");
      if (!Collection.class.isAssignableFrom(getShutdownHooksInOrder.getReturnType())) {
        LOG.warn("Unsupported method {}. Spark shutdown hooks cleanup skipped.", getShutdownHooksInOrder);
        return;
      }
      getShutdownHooksInOrder.setAccessible(true);

      // Filter out hooks that are defined in the same SparkRunnerClassLoader as this SparkProgramRunner class
      // This is for the case when there are concurrent Spark job running in the same VM
      for (Object hookEntry : (Collection<?>) getShutdownHooksInOrder.invoke(manager)) {
        Runnable runnable = getShutdownHookRunnable(hookEntry);
        if (runnable != null && runnable.getClass().getClassLoader() == getClass().getClassLoader()) {
          LOG.debug("Running Spark shutdown hook {}", runnable);
          runnable.run();
          manager.removeShutdownHook(runnable);
        }
      }
    } catch (Exception e) {
      LOG.warn("Failed to cleanup Spark shutdown hooks.", e);
    }
  }

  @Nullable
  private Runnable getShutdownHookRunnable(Object hookEntry) {
    try {
      // Pre Hadoop-2.8, the entry is the Runnable added to the ShutdownHookManager
      // Post Hadoop-2.8, the entry is the ShutdownHookManager.HookEntry class
      if (!(hookEntry instanceof Runnable)) {
        Field hookField = hookEntry.getClass().getDeclaredField("hook");
        hookField.setAccessible(true);
        hookEntry = hookField.get(hookEntry);
      }

      if (hookEntry instanceof Runnable) {
        return (Runnable) hookEntry;
      }

      throw new UnsupportedTypeException("Hook entry is not a Runnable: " + hookEntry.getClass().getName());
    } catch (Exception e) {
      LOG.warn("Failed to get Spark shutdown hook Runnable from Hadoop ShutdownHookManager hook entry {} due to {}",
               hookEntry, e.toString());
    }
    return null;
  }

  /**
   * Clear the constructor cache in the BeanIntrospector to avoid leaking ClassLoader.
   */
  private void invalidateBeanIntrospectorCache() {
    try {
      // Get the class through reflection since some Spark version doesn't depend on the fasterxml scala module.
      // This is to avoid class not found issue
      Class<?> cls = Class.forName("com.fasterxml.jackson.module.scala.introspect.BeanIntrospector$");
      Field field = Fields.findField(cls, "ctorParamNamesCache");

      // See if it is a LoadingCache. Need to check with class name since in distributed mode assembly jar, the
      // guava classes are shaded and renamed.
      switch (field.getType().getName()) {
        // Get the cache field and invalid it.
        // The BeanIntrospector is a scala object and scala generates a static MODULE$ field to the singleton object
        // We need to make both two different class name because in the Spark assembly jar, the guava classes
        // get renamed. In unit-test, however, the class won't get renamed because they are pulled from dependency
        case "com.google.common.cache.LoadingCache":
        case "org.spark-project.guava.cache.LoadingCache":
          field.setAccessible(true);
          Object cache = field.get(Fields.findField(cls, "MODULE$").get(null));
          // Try to call the invalidateAll method of the cache
          Method invalidateAll = cache.getClass().getMethod("invalidateAll");
          invalidateAll.setAccessible(true);
          invalidateAll.invoke(cache);
          LOG.debug("BeanIntrospector.ctorParamNamesCache has been invalidated.");
        break;

        default:
          // Unexpected, maybe due to version change in the BeanIntrospector, hence log a WARN.
          LOG.warn("BeanIntrospector.ctorParamNamesCache is not a LoadingCache, may lead to memory leak in SDK." +
                     "Field type is {}", field.getType());
      }
    } catch (NoSuchFieldException e) {
      // If there is no ctorParamNamesCache field, there is nothing to invalidate.
      // This is the case in jackson-module-scala_2.11-2.6.5 used by Spark 2.1.0
      LOG.trace("No ctorParamNamesCache field in BeanIntrospector. " +
                  "The current Spark version is not using a BeanIntrospector that has a param names loading cache.");
    } catch (ClassNotFoundException e) {
      // Catch the case when there is no BeanIntrospector class. It is ok since some Spark version may not be using it.
      LOG.debug("No BeanIntrospector class found. The current Spark version is not using BeanIntrospector.");
    } catch (Exception e) {
      LOG.warn("Failed to cleanup BeanIntrospector cache, may lead to memory leak in SDK.", e);
    }
  }

  /**
   * Creates a {@link ListenableFuture} that is cancelled.
   */
  private <V> ListenableFuture<V> immediateCancelledFuture() {
    SettableFuture<V> future = SettableFuture.create();
    future.cancel(true);
    return future;
  }

  /**
   * Prepares the {@link HBaseDDLExecutor} implementation for localization.
   */
  private void prepareHBaseDDLExecutorResources(File tempDir, CConfiguration cConf,
                                                List<LocalizeResource> localizeResources)
    throws IOException {
    String ddlExecutorExtensionDir = cConf.get(Constants.HBaseDDLExecutor.EXTENSIONS_DIR);
    if (ddlExecutorExtensionDir == null) {
      // Nothing to localize
      return;
    }

    final File target = new File(tempDir, "hbaseddlext.jar");
    BundleJarUtil.createJar(new File(ddlExecutorExtensionDir), target);
    localizeResources.add(new LocalizeResource(target, true));
    cConf.set(Constants.HBaseDDLExecutor.EXTENSIONS_DIR, target.getName());
  }
}
