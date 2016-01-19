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
package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.CombineClassLoader;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.twill.HadoopClassExcluder;
import co.cask.cdap.common.twill.LocalLocationFactory;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.internal.app.runtime.LocalizationUtils;
import co.cask.cdap.internal.app.runtime.distributed.LocalizeResource;
import co.cask.cdap.internal.app.runtime.spark.metrics.SparkMetricsSink;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
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
  private final Configuration hConf;
  private final Spark spark;
  private final Location programJarLocation;
  private final SparkContextFactory sparkContextFactory;
  private final SparkSubmitter sparkSubmitter;
  private final TransactionSystemClient txClient;
  private final AtomicReference<ExecutionFuture<RunId>> submissionFuture;
  private Callable<ExecutionFuture<RunId>> submitSpark;
  private Runnable cleanupTask;

  SparkRuntimeService(CConfiguration cConf, Configuration hConf, Spark spark,
                      SparkContextFactory sparkContextFactory, SparkSubmitter sparkSubmitter,
                      Location programJarLocation, TransactionSystemClient txClient) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.spark = spark;
    this.programJarLocation = programJarLocation;
    this.sparkContextFactory = sparkContextFactory;
    this.sparkSubmitter = sparkSubmitter;
    this.txClient = txClient;
    this.submissionFuture = new AtomicReference<>();
  }

  @Override
  protected String getServiceName() {
    return "Spark - " + sparkContextFactory.getClientContext().getSpecification().getName();
  }

  @Override
  protected void startUp() throws Exception {
    // additional spark job initialization at run-time
    beforeSubmit();

    // Creates a temporary directory locally for storing all generated files.
    File tempDir = DirUtils.createTempDir(new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                                   cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile());
    tempDir.mkdirs();
    this.cleanupTask = createCleanupTask(tempDir);
    try {
      SparkContextConfig contextConfig = new SparkContextConfig(hConf);
      ClientSparkContext clientContext = sparkContextFactory.getClientContext();

      final File jobJar = generateJobJar(tempDir);
      final List<LocalizeResource> localizeResources = new ArrayList<>();
      File pluginJar = clientContext.getPluginArchive();

      String metricsConfPath;
      String logbackJarName = null;

      if (contextConfig.isLocal()) {
        File metricsConf = SparkMetricsSink.writeConfig(File.createTempFile("metrics", ".properties", tempDir));
        metricsConfPath = metricsConf.getAbsolutePath();
      } else {
        // Localize files in distributed mode
        localizeResources.add(new LocalizeResource(copyProgramJar(programJarLocation, tempDir), true));
        localizeResources.add(new LocalizeResource(buildDependencyJar(tempDir), true));
        localizeResources.add(new LocalizeResource(saveCConf(cConf, tempDir)));

        if (pluginJar != null) {
          localizeResources.add(new LocalizeResource(pluginJar, true));
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
      }

      // Create a long running transaction
      Transaction transaction = txClient.startLong();

      Map<String, File> localizedFiles = localizeUserResources(contextConfig, clientContext,
                                                               localizeResources, tempDir);

      // Create execution spark context
      final ExecutionSparkContext executionContext =
        sparkContextFactory.createExecutionContext(transaction, localizedFiles);

      // Add additional resources that need to be localized to executor containers
      if (!contextConfig.isLocal()) {
        Configuration hConf = contextConfig.set(executionContext).getConfiguration();
        if (pluginJar != null) {
          hConf = new Configuration(hConf);
          hConf.set(Constants.Plugin.ARCHIVE, pluginJar.getName());
        }
        // also serialize the local resource names, so they can be recreated later.
        hConf.set(SparkContextProvider.LOCAL_RESOURCES, new Gson().toJson(localizedFiles.keySet()));
        localizeResources.add(new LocalizeResource(saveHConf(hConf, tempDir)));
      }

      final Map<String, String> configs = createSubmitConfigs(clientContext, tempDir, metricsConfPath, logbackJarName);
      submitSpark = new Callable<ExecutionFuture<RunId>>() {
        @Override
        public ExecutionFuture<RunId> call() throws Exception {
          return sparkSubmitter.submit(executionContext, configs,
                                       localizeResources, jobJar, executionContext.getRunId());
        }
      };
      submissionFuture.set(new SettableExecutionFuture<RunId>(executionContext));

    } catch (Throwable t) {
      cleanupTask.run();
      throw t;
    }
  }

  @Override
  protected void run() throws Exception {
    // First, only submit the job if this service is in RUNNING state.
    // This service will not be in running state if the triggeredShutdown() method
    // is invoked between startUp() and run() methods.
    //
    // The second clause is to guard against case where job is submitted through submitSpark.call(),
    // but before the atomic reference is updated, the triggeredShutdown() method is invoked and cancelled
    // the old Future reference inside the atomic reference. If that happened, need to cancel the submitted job.
    if (isRunning() && submissionFuture.getAndSet(submitSpark.call()).isCancelled()) {
      submissionFuture.get().cancel(true);
    }

    ExecutionFuture<RunId> jobCompletion = submissionFuture.get();
    ExecutionSparkContext executionContext = jobCompletion.getSparkContext();
    Transaction transaction = executionContext.getTransaction();

    try {
      // Block for job completion
      jobCompletion.get();
      try {
        executionContext.flushDatasets();
        LOG.debug("Committing Spark Program transaction: {}", executionContext);
        if (!txClient.commit(transaction)) {
          LOG.warn("Spark Job transaction failed to commit");
          throw new TransactionFailureException("Failed to commit transaction for Spark " + executionContext);
        }
        LOG.debug("Spark Program transaction committed: {}", executionContext);
      } catch (Exception e) {
        LOG.error("Failed to commit datasets", e);
        txClient.invalidate(transaction.getWritePointer());
      }

    } catch (Exception e) {
      // invalidate the transaction as spark might have written to datasets too
      txClient.invalidate(transaction.getWritePointer());

      // See if it is due to job cancelation. If it is, then it's not an error.
      if (jobCompletion.isCancelled()) {
        LOG.debug("Spark program execution cancelled: {}", sparkContextFactory.getClientContext());
      } else {
        LOG.error("Spark program execution failure: {}", sparkContextFactory.getClientContext(), e);
        throw e;
      }
    } finally {
      Closeables.closeQuietly(executionContext);
    }
  }

  @Override
  protected void shutDown() throws Exception {
    try {
      // Try to get from the submission future to see if the job completed successfully.
      boolean success = true;
      try {
        submissionFuture.get().get();
      } catch (Exception e) {
        success = false;
      }
      onFinish(success);
    } finally {
      cleanupTask.run();
      LOG.debug("Spark program completed: {}", sparkContextFactory.getClientContext());
    }
  }

  @Override
  protected void triggerShutdown() {
    LOG.debug("Stop requested for Spark Program {}", submissionFuture.get().getSparkContext());
    submissionFuture.get().cancel(true);
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
            LoggingContextAccessor.setLoggingContext(sparkContextFactory.getClientContext().getLoggingContext());
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
   * Calls the {@link Spark#beforeSubmit(SparkContext)} method.
   */
  private void beforeSubmit() throws TransactionFailureException, InterruptedException {
    TransactionContext txContext = sparkContextFactory.getClientContext().getTransactionContext();
    Transactions.execute(txContext, spark.getClass().getName() + ".beforeSubmit()", new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(new CombineClassLoader(
          null, ImmutableList.of(spark.getClass().getClassLoader(), getClass().getClassLoader())));
        try {
          spark.beforeSubmit(sparkContextFactory.getClientContext());
          return null;
        } finally {
          ClassLoaders.setContextClassLoader(oldClassLoader);
        }
      }
    });
  }

  /**
   * Calls the {@link Spark#onFinish(boolean, SparkContext)} method.
   */
  private void onFinish(final boolean succeeded) throws TransactionFailureException, InterruptedException {
    TransactionContext txContext = sparkContextFactory.getClientContext().getTransactionContext();
    Transactions.execute(txContext, spark.getClass().getName() + ".onFinish()", new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(new CombineClassLoader(
          null, ImmutableList.of(spark.getClass().getClassLoader(), getClass().getClassLoader())));
        try {
          spark.onFinish(succeeded, sparkContextFactory.getClientContext());
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
  private Map<String, String> createSubmitConfigs(ClientSparkContext context, File localDir, String metricsConfPath,
                                                  @Nullable String logbackJarName) {
    Map<String, String> configs = new HashMap<>();

    // Add user specified configs first. CDAP specifics config will override them later if there are duplicates.
    for (Tuple2<String, String> tuple : context.getSparkConf().getAll()) {
      configs.put(tuple._1(), tuple._2());
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
    appBundler.createBundle(tempLocation, SparkProgramWrapper.class, HBaseTableUtilFactory.getHBaseTableUtilClass());
    return new File(Locations.toURI(tempLocation));
  }

  /**
   * Copy the program jar to local directory.
   *
   * @return {@link File} of the copied jar in the given target directory
   */
  private File copyProgramJar(Location programJar, File targetDir) throws IOException {
    File tempFile = new File(targetDir, SparkContextProvider.PROGRAM_JAR_NAME);

    LOG.debug("Copy program jar from {} to {}", programJar, tempFile);
    Files.copy(Locations.newInputSupplier(programJar), tempFile);
    return tempFile;
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
    File file = new File(targetDir, SparkContextProvider.CCONF_FILE_NAME);
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
    File file = new File(targetDir, SparkContextProvider.HCONF_FILE_NAME);
    try (Writer writer = Files.newWriter(file, Charsets.UTF_8)) {
      hConf.writeXml(writer);
    }
    return file;
  }

  /**
   * Localizes resources requested by users in the Spark Program's client (or beforeSubmit) phases.
   *
   * @param contextConfig the {@link SparkContextConfig} for this Spark program
   * @param clientContext the {@link ClientSparkContext} for this Spark program
   * @param allLocalizedResources the list of all (user-requested + CDAP system) {@link LocalizeResource} to be
   *                                 localized for this Spark program
   * @param targetDir a temporary directory to copy the resources to
   * @return a {@link Map} of resource name to the {@link File} handle for the local file.
   */
  private Map<String, File> localizeUserResources(SparkContextConfig contextConfig,
                                                  ClientSparkContext clientContext,
                                                  List<LocalizeResource> allLocalizedResources,
                                                  File targetDir) throws IOException {
    Map<String, File> localizedResources = new HashMap<>();
    Map<String, LocalizeResource> resourcesToLocalize = clientContext.getResourcesToLocalize();
    for (final Map.Entry<String, LocalizeResource> entry : resourcesToLocalize.entrySet()) {
      // localize resources in a temporary directory
      File localizedFile = LocalizationUtils.localizeResource(entry.getKey(), entry.getValue(), targetDir);

      if (!contextConfig.isLocal()) {
        // in distributed mode, file localization happens in the Spark Submitter, so add it to the
        // allLocalizedResources list
        try {
          URI uri = localizedFile.toURI();
          URI actualURI = new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), uri.getQuery(), entry.getKey());
          allLocalizedResources.add(new LocalizeResource(actualURI, entry.getValue().isArchive()));
        } catch (URISyntaxException e) {
          // Most of the URI is constructed from the passed URI. So ideally, this should not happen.
          // If it does though, there is nothing that clients can do to recover, so not propagating a checked exception.
          throw Throwables.propagate(e);
        }
      }
      localizedResources.put(entry.getKey(), localizedFile);
    }
    return localizedResources;
  }

  /**
   * Creates a {@link Runnable} for deleting content for the given directory.
   */
  private Runnable createCleanupTask(final File directory) {
    return new Runnable() {

      @Override
      public void run() {
        // Cleanup all system properties setup by SparkSubmit
        Iterable<String> sparkKeys = Iterables.filter(System.getProperties().stringPropertyNames(),
                                                      Predicates.containsPattern("^spark\\."));
        for (String key : sparkKeys) {
          LOG.debug("Removing Spark system property: {}", key);
          System.clearProperty(key);
        }

        try {
          DirUtils.deleteDirectoryContents(directory);
        } catch (IOException e) {
          LOG.warn("Failed to cleanup directory {}", directory);
        }
      }
    };
  }
}
