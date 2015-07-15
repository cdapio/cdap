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
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.internal.app.runtime.spark.metrics.SparkMetricsSink;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.deploy.SparkSubmit;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.ApplicationBundler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.jar.JarOutputStream;

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
  private static final Function<File, String> FILE_TO_ABSOLUTE_PATH = new Function<File, String>() {
    @Override
    public String apply(File input) {
      return input.getAbsolutePath();
    }
  };

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final Spark spark;
  private final Location programJarLocation;
  private final SparkContextFactory sparkContextFactory;
  private final TransactionSystemClient txClient;
  private ExecutionSparkContext executionContext;
  private Runnable cleanupTask;
  private String[] sparkSubmitArgs;
  private boolean success;

  SparkRuntimeService(CConfiguration cConf, Configuration hConf, Spark spark,
                      SparkContextFactory sparkContextFactory, Location programJarLocation,
                      TransactionSystemClient txClient) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.spark = spark;
    this.programJarLocation = programJarLocation;
    this.sparkContextFactory = sparkContextFactory;
    this.txClient = txClient;
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

      File jobJar = generateJobJar(tempDir);
      File metricsConf = SparkMetricsSink.generateSparkMetricsConfig(File.createTempFile("metrics",
                                                                                         ".properties", tempDir));

      List<File> localizedArchives = Lists.newArrayList();
      List<File> localizedFiles = Lists.newArrayList();

      if (!contextConfig.isLocal()) {
        localizedArchives.add(copyProgramJar(programJarLocation, tempDir));
        localizedArchives.add(buildDependencyJar(tempDir));
        localizedFiles.add(saveCConf(cConf, tempDir));
      }

      // Create a long running transaction
      Transaction transaction = txClient.startLong();

      // Create execution spark context
      executionContext = sparkContextFactory.createExecutionContext(transaction);

      if (!contextConfig.isLocal()) {
        localizedFiles.add(saveHConf(contextConfig.set(executionContext).getConfiguration(), tempDir));
      }

      sparkSubmitArgs = prepareSparkSubmitArgs(contextConfig.getExecutionMode(), sparkContextFactory.getClientContext(),
                                               tempDir, localizedArchives, localizedFiles, jobJar, metricsConf);

    } catch (Throwable t) {
      cleanupTask.run();
      throw t;
    }
  }

  @Override
  protected void run() throws Exception {
    SparkClassLoader sparkClassLoader = new SparkClassLoader(executionContext);
    ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(sparkClassLoader);
    try {
      LOG.debug("Submitting to spark with arguments: {}", Arrays.toString(sparkSubmitArgs));
      SparkSubmit.main(sparkSubmitArgs);
      success = true;
    } catch (Exception e) {
      success = false;
      // See if the context is stopped. If it is, then it's not an error.
      if (!executionContext.isStopped()) {
        LOG.error("Spark program execution failure: {}", executionContext, e);
        throw e;
      }
    } finally {
      Thread.currentThread().setContextClassLoader(oldClassLoader);
    }
  }

  @Override
  protected void shutDown() throws Exception {
    try {
      Transaction transaction = executionContext.getTransaction();
      if (success) {
        try {
          executionContext.flushDatasets();
          LOG.debug("Committing Spark Program transaction: {}", executionContext);
          if (!txClient.commit(transaction)) {
            LOG.warn("Spark Job transaction failed to commit");
            throw new TransactionFailureException("Failed to commit transaction for Spark " + executionContext);
          }
        } catch (Exception e) {
          LOG.error("Failed to commit datasets", e);
          txClient.invalidate(transaction.getWritePointer());
        }
      } else {
        // invalidate the transaction as spark might have written to datasets too
        txClient.invalidate(transaction.getWritePointer());
      }
    } finally {
      // whatever happens we want to call this
      try {
        onFinish(success);
      } finally {
        Closeables.closeQuietly(executionContext);
        Closeables.closeQuietly(sparkContextFactory.getClientContext());
        cleanupTask.run();
      }
    }
  }

  @Override
  protected void triggerShutdown() {
    // No need to synchronize. The guava Service guarantees this method won't get called until
    // the startUp() call returned.
    ExecutionSparkContext context = executionContext;
    if (context != null) {
      context.close();
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
   * Prepares arguments which {@link SparkProgramWrapper} is submitted to {@link SparkSubmit} to run.
   *
   * @param executionMode name of the execution mode
   * @param context the {@link ClientSparkContext} for this execution
   * @param localDir the directory to be used as the spark local directory
   * @param localizedArchives list of files to be localized to the remote container as archives (.jar wil be expanded)
   * @param localizedFiles list of files to be localized to the remote container
   * @param jobJar the jar file submitted to Spark as the job jar
   * @param metricsConf the configuration file for specifying Spark framework metrics collection integration
   *
   * @return String[] of arguments with which {@link SparkProgramWrapper} will be submitted
   */
  private String[] prepareSparkSubmitArgs(String executionMode, ClientSparkContext context, File localDir,
                                          Iterable<File> localizedArchives, Iterable<File> localizedFiles,
                                          File jobJar, File metricsConf) {

    String archives = Joiner.on(',').join(Iterables.transform(localizedArchives, FILE_TO_ABSOLUTE_PATH));
    String files = Joiner.on(',').join(Iterables.transform(localizedFiles, FILE_TO_ABSOLUTE_PATH));

    ImmutableList.Builder<String> builder = ImmutableList.builder();

    // Add user specified configs first. CDAP specifics config will override them later if there are duplicates.
    for (Tuple2<String, String> tuple : context.getSparkConf().getAll()) {
      builder.add("--conf").add(tuple._1() + "=" + tuple._2());
    }

    builder.add("--class").add(SparkProgramWrapper.class.getName())
           .add("--master").add(executionMode)
           .add("--conf").add("spark.executor.extraClassPath=$PWD/" + CDAP_SPARK_JAR + "/lib/*")
           .add("--conf").add("spark.metrics.conf=" + metricsConf.getAbsolutePath())
           .add("--conf").add("spark.local.dir=" + localDir.getAbsolutePath())
           .add("--conf").add("spark.executor.memory=" + context.getExecutorResources().getMemoryMB() + "m")
           .add("--conf").add("spark.executor.cores=" + context.getExecutorResources().getVirtualCores());

    if (!archives.isEmpty()) {
      builder.add("--archives").add(archives);
    }
    if (!files.isEmpty()) {
      builder.add("--files").add(files);
    }
    List<String> args = builder
      .add(jobJar.getAbsolutePath())
      .add(context.getSpecification().getMainClassName())
      .build();

    return args.toArray(new String[args.size()]);
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
    return new File(tempLocation.toURI());
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
   * Creates a {@link Runnable} for deleting content for the given directory.
   */
  private Runnable createCleanupTask(final File directory) {
    return new Runnable() {

      @Override
      public void run() {
        try {
          DirUtils.deleteDirectoryContents(directory);
        } catch (IOException e) {
          LOG.warn("Failed to cleanup directory {}", directory);
        }
      }
    };
  }
}
