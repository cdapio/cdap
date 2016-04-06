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

import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkClientContext;
import co.cask.cdap.api.spark.SparkExecutionContext;
import co.cask.cdap.app.runtime.spark.submit.SparkSubmitter;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.CombineClassLoader;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.twill.HadoopClassExcluder;
import co.cask.cdap.common.twill.LocalLocationFactory;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.internal.app.runtime.LocalizationUtils;
import co.cask.cdap.internal.app.runtime.distributed.LocalizeResource;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.SparkSubmit;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.api.RunId;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.ApplicationBundler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import javax.annotation.Nullable;

/**
 * Performs the actual execution of Spark job.
 * <p/>
 * Service start -> Performs job setup, and beforeSubmit call.
 * Service run -> Submits the spark job through {@link SparkSubmit}
 * Service triggerStop -> kill job
 * Service stop -> Commit/invalidate transaction, onFinish, cleanup
 */
final class SparkRuntimeService extends AbstractExecutionThreadService {

  private static final String CDAP_SPARK_JAR = "cdap-spark.jar";

  private static final Logger LOG = LoggerFactory.getLogger(SparkRuntimeService.class);

  private final CConfiguration cConf;
  private final Spark spark;
  private final SparkRuntimeContext runtimeContext;
  private final File pluginArchive;
  private final SparkSubmitter sparkSubmitter;
  private final AtomicReference<ListenableFuture<RunId>> completion;
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
  }

  @Override
  protected String getServiceName() {
    return "Spark - " + runtimeContext.getSparkSpecification().getName();
  }

  @Override
  protected void startUp() throws Exception {
    // additional spark job initialization at run-time
    // This context is for calling beforeSubmit and onFinish on the Spark program
    // TODO: Refactor the ClientSparkContext class to reduce it's complexity
    BasicSparkClientContext context = new BasicSparkClientContext(runtimeContext);
    beforeSubmit(context);

    // Creates a temporary directory locally for storing all generated files.
    File tempDir = DirUtils.createTempDir(new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                                   cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile());
    tempDir.mkdirs();
    this.cleanupTask = createCleanupTask(tempDir, System.getProperties());
    try {
      SparkRuntimeContextConfig contextConfig = new SparkRuntimeContextConfig(runtimeContext.getConfiguration());

      final File jobJar = generateJobJar(tempDir);
      final List<LocalizeResource> localizeResources = new ArrayList<>();

      String metricsConfPath;
      String logbackJarName = null;

      // Always copy (or link if local) user requested resources to local node. This is needed because the
      // driver is running in the same container as the launcher (i.e. this process).
      final Map<String, File> localizedFiles = copyUserResources(context.getLocalizeResources(), tempDir);
      final SparkExecutionContextFactory contextFactory = new SparkExecutionContextFactory() {
        @Override
        public SparkExecutionContext create(SparkRuntimeContext runtimeContext) {
          return new DefaultSparkExecutionContext(runtimeContext, localizedFiles);
        }
      };

      if (contextConfig.isLocal()) {
        File metricsConf = SparkMetricsSink.writeConfig(File.createTempFile("metrics", ".properties", tempDir));
        metricsConfPath = metricsConf.getAbsolutePath();
      } else {
        // Localize all user requested files in distributed mode
        distributedUserResources(context.getLocalizeResources(), localizeResources);

        // Localize system files in distributed mode
        File programJar = Locations.linkOrCopy(runtimeContext.getProgram().getJarLocation(),
                                               new File(tempDir, SparkRuntimeContextProvider.PROGRAM_JAR_NAME));
        File expandedProgramJar = Locations.linkOrCopy(runtimeContext.getProgram().getJarLocation(),
                                                       new File(tempDir,
                                                                SparkRuntimeContextProvider.PROGRAM_JAR_EXPANDED_NAME));
        // Localize both the unexpanded and expanded jar
        localizeResources.add(new LocalizeResource(programJar, false));
        localizeResources.add(new LocalizeResource(expandedProgramJar, true));
        localizeResources.add(new LocalizeResource(buildDependencyJar(tempDir), true));
        localizeResources.add(new LocalizeResource(saveCConf(cConf, tempDir)));

        if (pluginArchive != null) {
          localizeResources.add(new LocalizeResource(pluginArchive, true));
        }

        File logbackJar = createLogbackJar(tempDir);
        if (logbackJar != null) {
          localizeResources.add(new LocalizeResource(logbackJar));
          logbackJarName = logbackJar.getName();
        }

        // Create metrics conf file in the current directory since
        // the same value for the "spark.metrics.conf" config needs to be used for both driver and executor processes
        // Also localize the metrics conf file to the executor nodes
        File metricsConf = SparkMetricsSink.writeConfig(File.createTempFile("metrics", ".properties",
                                                                            new File(System.getProperty("user.dir"))));
        metricsConfPath = metricsConf.getName();
        localizeResources.add(new LocalizeResource(metricsConf));

        // Preserves runtime information in the hConf
        Configuration hConf = contextConfig.set(runtimeContext,
                                                localizedFiles.keySet(), pluginArchive).getConfiguration();

        // Localize the hConf file to executor nodes
        localizeResources.add(new LocalizeResource(saveHConf(hConf, tempDir)));
      }

      final Map<String, String> configs = createSubmitConfigs(context, tempDir, metricsConfPath, logbackJarName);
      submitSpark = new Callable<ListenableFuture<RunId>>() {
        @Override
        public ListenableFuture<RunId> call() throws Exception {
          // If stop already requested on this service, don't submit the spark.
          // This happen when stop() was called whiling starting
          if (!isRunning()) {
            return immediateCancelledFuture();
          }
          return sparkSubmitter.submit(runtimeContext, contextFactory, configs,
                                       localizeResources, jobJar, runtimeContext.getRunId());
        }
      };
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
        LOG.debug("Spark program execution cancelled: {}", runtimeContext);
      } else {
        LOG.error("Spark program execution failure: {}", runtimeContext, e);
        throw e;
      }
    }
  }

  @Override
  protected void shutDown() throws Exception {
    // Try to get from the submission future to see if the job completed successfully.
    ListenableFuture<RunId> jobCompletion = completion.get();
    boolean succeeded = true;
    try {
      jobCompletion.get();
    } catch (Exception e) {
      succeeded = false;
    }

    try {
      onFinish(succeeded, new BasicSparkClientContext(runtimeContext));
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
        t.setName(getServiceName());
        t.start();
      }
    };
  }

  /**
   * Calls the {@link Spark#beforeSubmit(SparkClientContext)} method.
   */
  private void beforeSubmit(final SparkClientContext context) throws TransactionFailureException, InterruptedException {
    DynamicDatasetCache datasetCache = runtimeContext.getDatasetCache();
    TransactionContext txContext = datasetCache.newTransactionContext();
    Transactions.execute(txContext, spark.getClass().getName() + ".beforeSubmit()", new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(new CombineClassLoader(
          null, ImmutableList.of(spark.getClass().getClassLoader(), getClass().getClassLoader())));
        try {
          spark.beforeSubmit(context);
          return null;
        } finally {
          ClassLoaders.setContextClassLoader(oldClassLoader);
        }
      }
    });
  }

  /**
   * Calls the {@link Spark#onFinish(boolean, SparkClientContext)} method.
   */
  private void onFinish(final boolean succeeded,
                        final SparkClientContext context) throws TransactionFailureException, InterruptedException {
    DynamicDatasetCache datasetCache = runtimeContext.getDatasetCache();
    TransactionContext txContext = datasetCache.newTransactionContext();

    Transactions.execute(txContext, spark.getClass().getName() + ".onFinish()", new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(new CombineClassLoader(
          null, ImmutableList.of(spark.getClass().getClassLoader(), getClass().getClassLoader())));
        try {
          spark.onFinish(succeeded, context);
          return null;
        } finally {
          ClassLoaders.setContextClassLoader(oldClassLoader);
        }
      }
    });
  }

  /**
   * Creates the configurations for the spark submitter.
   */
  private Map<String, String> createSubmitConfigs(BasicSparkClientContext context, File localDir,
                                                  String metricsConfPath, @Nullable String logbackJarName) {
    Map<String, String> configs = new HashMap<>();

    // Add user specified configs first. CDAP specifics config will override them later if there are duplicates.
    SparkConf sparkConf = context.getSparkConf();
    if (sparkConf != null) {
      for (Tuple2<String, String> tuple : sparkConf.getAll()) {
        configs.put(tuple._1(), tuple._2());
      }
    }

    String extraClassPath = "$PWD/" + CDAP_SPARK_JAR + "/lib/*";
    if (logbackJarName != null) {
      extraClassPath = logbackJarName + File.pathSeparator + extraClassPath;
    }
    configs.put("spark.executor.extraClassPath", extraClassPath);
    configs.put("spark.metrics.conf", metricsConfPath);
    configs.put("spark.local.dir", localDir.getAbsolutePath());
    configs.put("spark.executor.memory", context.getExecutorResources().getMemoryMB() + "m");
    configs.put("spark.executor.cores", String.valueOf(context.getExecutorResources().getVirtualCores()));

    return configs;
  }

  /**
   * Packages all the dependencies of the Spark job. It contains all CDAP classes that are needed to run the
   * user spark program.
   *
   * @param targetDir directory for the file to be created in
   * @return {@link File} of the dependency jar in the given target directory
   * @throws IOException if failed to package the jar
   */
  private File buildDependencyJar(File targetDir) throws IOException {
    Location tempLocation = new LocalLocationFactory(targetDir).create(CDAP_SPARK_JAR);

    final HadoopClassExcluder hadoopClassExcluder = new HadoopClassExcluder();
    ApplicationBundler appBundler = new ApplicationBundler(new ClassAcceptor() {

      @Override
      public boolean accept(String className, URL classUrl, URL classPathUrl) {
        // Exclude the spark-assembly and scala
        if (className.startsWith("org.apache.spark")
          || className.startsWith("scala")
          || classPathUrl.toString().contains("spark-assembly")) {
          return false;
        }
        return hadoopClassExcluder.accept(className, classUrl, classPathUrl);
      }
    });
    appBundler.createBundle(tempLocation, SparkMainWrapper.class, HBaseTableUtilFactory.getHBaseTableUtilClass());
    return new File(tempLocation.toURI());
  }

  /**
   * Generates an empty JAR file.
   *
   * @return The generated {@link File} in the given target directory
   */
  private File generateJobJar(File targetDir) throws IOException {
    File tempFile = new File(targetDir, "emptyJob.jar");
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
   * Creates a jar in the given directory that contains a logback.xml loaded from the current ClassLoader.
   *
   * @param targetDir directory where the logback.xml should be copied to
   * @return the {@link File} where the logback.xml jar copied to or {@code null} if "logback.xml" is not found
   *         in the current ClassLoader.
   */
  @Nullable
  private File createLogbackJar(File targetDir) throws IOException {
    // Localize logback.xml
    ClassLoader cl = Objects.firstNonNull(Thread.currentThread().getContextClassLoader(), getClass().getClassLoader());
    try (InputStream input = cl.getResourceAsStream("logback.xml")) {
      if (input != null) {
        File logbackJar = File.createTempFile("logback.xml", ".jar", targetDir);
        try (JarOutputStream output = new JarOutputStream(new FileOutputStream(logbackJar))) {
          output.putNextEntry(new JarEntry("logback.xml"));
          ByteStreams.copy(input, output);
        }
        return logbackJar;
      } else {
        LOG.warn("Could not find logback.xml for Spark!");
      }
    }
    return null;
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
   * @return a map from resource name to local file path.
   */
  private Map<String, File> copyUserResources(Map<String, LocalizeResource> resources,
                                              File targetDir) throws IOException {
    Map<String, File> result = new HashMap<>();
    for (Map.Entry<String, LocalizeResource> entry : resources.entrySet()) {
      result.put(entry.getKey(), LocalizationUtils.localizeResource(entry.getKey(), entry.getValue(), targetDir));
    }
    return result;
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
   * Creates a {@link ListenableFuture} that is cancelled.
   */
  private <V> ListenableFuture<V> immediateCancelledFuture() {
    SettableFuture<V> future = SettableFuture.create();
    future.cancel(true);
    return future;
  }
}
