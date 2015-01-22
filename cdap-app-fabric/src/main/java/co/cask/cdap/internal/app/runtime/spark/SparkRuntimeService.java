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
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.runtime.spark.metrics.SparkMetricsSink;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.tephra.DefaultTransactionExecutor;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import com.clearspring.analytics.util.Lists;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.spark.deploy.SparkSubmit;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * Performs the actual execution of Spark job.
 * <p/>
 * Service start -> Performs job setup, beforeSubmit and submit job
 * Service run -> Submits the spark job through {@link SparkSubmit}
 * Service triggerStop -> kill job
 * Service stop -> Commit/invalidate transaction, onFinish, cleanup
 */
final class SparkRuntimeService extends AbstractExecutionThreadService {

  static final String SPARK_HCONF_FILENAME = "spark_hconf.xml";
  private static final Logger LOG = LoggerFactory.getLogger(SparkRuntimeService.class);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final Spark spark;
  private final SparkSpecification sparkSpecification;
  private final Location programJarLocation;
  private final BasicSparkContext context;
  private final LocationFactory locationFactory;
  private final TransactionSystemClient txClient;
  private Transaction transaction;
  private Runnable cleanupTask;
  private String[] sparkSubmitArgs;
  private volatile boolean stopRequested;

  SparkRuntimeService(CConfiguration cConf, Configuration hConf, Spark spark, SparkSpecification sparkSpecification,
                      BasicSparkContext context, Location programJarLocation, LocationFactory locationFactory,
                      TransactionSystemClient txClient) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.spark = spark;
    this.sparkSpecification = sparkSpecification;
    this.programJarLocation = programJarLocation;
    this.context = context;
    this.locationFactory = locationFactory;
    this.txClient = txClient;
  }

  @Override
  protected String getServiceName() {
    return "Spark - " + sparkSpecification.getName();
  }

  @Override
  protected void startUp() throws Exception {
    Configuration sparkHConf = new Configuration(hConf);

    // additional spark job initialization at run-time
    beforeSubmit();

    try {
      Location programJarCopy = copyProgramJar(programJarLocation, context);
      try {
        // We remember tx, so that we can re-use it in Spark tasks
        Transaction tx = txClient.startLong();
        try {
          SparkContextConfig.set(sparkHConf, context, cConf, tx, programJarCopy);
          Location dependencyJar = buildDependencyJar(context, SparkContextConfig.getHConf());
          try {
            File tmpFile = File.createTempFile(SparkMetricsSink.SPARK_METRICS_PROPERTIES_FILENAME,
                                               Location.TEMP_FILE_SUFFIX,
                                               new File(new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR)),
                                                        cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile());
            try {
              SparkMetricsSink.generateSparkMetricsConfig(tmpFile);
              context.setMetricsPropertyFile(tmpFile);
              sparkSubmitArgs = prepareSparkSubmitArgs(sparkSpecification, sparkHConf, programJarCopy, dependencyJar);
              LOG.info("Submitting Spark program: {} with arguments {}", context, Arrays.toString(sparkSubmitArgs));
              this.transaction = tx;
              this.cleanupTask = createCleanupTask(ImmutableList.of(tmpFile),
                                                   ImmutableList.of(dependencyJar, programJarCopy));
            } catch (Throwable t) {
              tmpFile.delete();
              throw t;
            }
          } catch (Throwable t) {
            LOG.error("Exception while creating metrics properties file for Spark", t);
            Locations.deleteQuietly(dependencyJar);
            throw t;
          }
        } catch (Throwable t) {
          Transactions.invalidateQuietly(txClient, tx);
          throw t;
        }
      } catch (Throwable t) {
        Locations.deleteQuietly(programJarCopy);
        throw t;
      }
    } catch (Throwable t) {
      LOG.error("Exception while preparing for submitting Spark Job: {}", context, t);
      throw Throwables.propagate(t);
    }
  }

  @Override
  protected void run() throws Exception {
    try {
      ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
      // TODO: CDAP-598. Currently set it to CDAP system ClassLoader so that all system classes are available
      // to the Spark program. The program classes are loaded from the JAR created through the copyProgramJar method.
      // With this, the user Spark program can run, but it cannot use library of different version that CDAP
      // depends on.
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      try {
        SparkProgramWrapper.setBasicSparkContext(context);
        SparkProgramWrapper.setSparkProgramRunning(true);
        SparkSubmit.main(sparkSubmitArgs);
      } catch (Exception e) {
        LOG.error("Failed to submit Spark program {}", context, e);
      } finally {
        // job completed so update running status and get the success status
        SparkProgramWrapper.setSparkProgramRunning(false);
        Thread.currentThread().setContextClassLoader(oldClassLoader);
      }
    } catch (Exception e) {
      LOG.warn("Failed to set the classloader for submitting spark program");
      throw Throwables.propagate(e);
    }

    // If the job is not successful, throw exception so that this service will terminate with a failure state
    // Shutdown will still get executed, but the service will notify failure after that.
    // However, if it's the job is requested to stop (via triggerShutdown, meaning it's a user action), don't throw
    if (!stopRequested) {
      // if spark program is not running anymore and it was successful we can say the the program succeeded
      boolean programStatus = (!SparkProgramWrapper.isSparkProgramRunning()) &&
        SparkProgramWrapper.isSparkProgramSuccessful();
      Preconditions.checkState(programStatus, "Spark program execution failed.");
    }
  }

  @Override
  protected void shutDown() throws Exception {
    boolean success = SparkProgramWrapper.isSparkProgramSuccessful();
    try {
      if (success) {
        LOG.info("Committing Spark Program transaction: {}", context);
        if (!txClient.commit(transaction)) {
          LOG.warn("Spark Job transaction failed to commit");
          throw new TransactionFailureException("Failed to commit transaction for Spark " + context.toString());
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
        context.close();
        cleanupTask.run();
      }
    }
  }

  @Override
  protected void triggerShutdown() {
    try {
      stopRequested = true;
      if (SparkProgramWrapper.isSparkProgramRunning()) {
        SparkProgramWrapper.stopSparkProgram();
      }
    } catch (Exception e) {
      LOG.error("Failed to stop Spark job {}", sparkSpecification.getName(), e);
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
   * Calls the {@link Spark#beforeSubmit(SparkContext)} method.
   */
  private void beforeSubmit() throws TransactionFailureException, InterruptedException {
    createTransactionExecutor().execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(spark.getClass().getClassLoader());
        try {
          spark.beforeSubmit(context);
        } finally {
          Thread.currentThread().setContextClassLoader(oldClassLoader);
        }
      }
    });
  }

  /**
   * Calls the {@link Spark#onFinish(boolean, SparkContext)} method.
   */
  private void onFinish(final boolean succeeded) throws TransactionFailureException, InterruptedException {
    createTransactionExecutor().execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        spark.onFinish(succeeded, context);
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
   * Prepares arguments which {@link SparkProgramWrapper} is submitted to {@link SparkSubmit} to run.
   *
   * @param sparkSpec     {@link SparkSpecification} of this job
   * @param conf          {@link Configuration} of the job whose {@link MRConfig#FRAMEWORK_NAME} specifies the mode in
   *                      which spark runs
   * @param jobJarCopy    {@link Location} copy of user program
   * @param dependencyJar {@link Location} jar containing the dependencies of this job
   * @return String[] of arguments with which {@link SparkProgramWrapper} will be submitted
   */
  private String[] prepareSparkSubmitArgs(SparkSpecification sparkSpec, Configuration conf, Location jobJarCopy,
                                          Location dependencyJar) throws Exception {

    List<String> jars = Lists.newArrayList();
    jars.add(dependencyJar.toURI().getPath());

    // Spark doesn't support bundle jar. However, the ProgramClassLoader already has everything expanded.
    // We can just add those jars as dependency for spark submit.
    // It is a little hacky since we know the ProgramClassLoader is a URLClassLoader. Revisit in CDAP-598
    ClassLoader programClassLoader = spark.getClass().getClassLoader();
    if (programClassLoader instanceof URLClassLoader) {
      for (URL url : ((URLClassLoader) programClassLoader).getURLs()) {
        File file = new File(url.toURI()).getAbsoluteFile();
        if (file.isFile() && file.getName().endsWith(".jar")) {
          jars.add(file.getAbsolutePath());
        }
      }
    }

    return new String[]{"--class", SparkProgramWrapper.class.getCanonicalName(), "--jars",
      Joiner.on(',').join(jars), "--master", conf.get(MRConfig.FRAMEWORK_NAME), jobJarCopy.toURI().getPath(),
      sparkSpec.getMainClassName()};
  }

  /**
   * Packages all the dependencies of the Spark job
   *
   * @param context {@link BasicSparkContext} created for this job
   * @param conf    {@link Configuration} prepared for this job by {@link SparkContextConfig}
   * @return {@link Location} of the dependency jar
   * @throws IOException if failed to package the jar
   */
  private Location buildDependencyJar(BasicSparkContext context, Configuration conf) throws IOException {
    Id.Program programId = context.getProgram().getId();

    Location jobJarLocation = locationFactory.create(String.format("%s.%s.%s.%s.%s.jar",
                                                                   ProgramType.SPARK.name().toLowerCase(),
                                                                   programId.getNamespaceId(),
                                                                   programId.getApplicationId(), programId.getId(),
                                                                   context.getRunId().getId()));

    LOG.debug("Creating Spark Job Jar: {}", jobJarLocation.toURI());
    JarOutputStream jarOut = new JarOutputStream(jobJarLocation.getOutputStream());
    try {
      // All we need is the serialized hConf in the job jar
      jarOut.putNextEntry(new JarEntry(SPARK_HCONF_FILENAME));
      conf.writeXml(jarOut);
    } finally {
      Closeables.closeQuietly(jarOut);
    }

    return jobJarLocation;
  }

  /**
   * Copies the user submitted program jar and flatten it out by expanding all jars in the "/" and "/lib" to top level.
   *
   * @param jobJarLocation {link Location} of the user's job
   * @param context        {@link BasicSparkContext} context of this job
   * @return {@link Location} where the program jar was copied
   * @throws IOException if failed to get the {@link Location#getInputStream()} or {@link Location#getOutputStream()}
   */
  private Location copyProgramJar(Location jobJarLocation, BasicSparkContext context) throws IOException {
    Id.Program programId = context.getProgram().getId();
    Location programJarCopy = locationFactory.create(String.format("%s.%s.%s.%s.%s.program.jar",
                                                                   ProgramType.SPARK.name().toLowerCase(),
                                                                   programId.getNamespaceId(),
                                                                   programId.getApplicationId(), programId.getId(),
                                                                   context.getRunId().getId()));

    ByteStreams.copy(Locations.newInputSupplier(jobJarLocation), Locations.newOutputSupplier(programJarCopy));
    return programJarCopy;
  }

  private Runnable createCleanupTask(final Iterable<File> files, final Iterable<Location> locations) {
    return new Runnable() {

      @Override
      public void run() {
        for (File file : files) {
          file.delete();
        }
        for (Location location : locations) {
          Locations.deleteQuietly(location);
        }
      }
    };
  }
}
