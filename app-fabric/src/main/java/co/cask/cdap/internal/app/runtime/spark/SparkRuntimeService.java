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
package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.CombineClassLoader;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.internal.app.runtime.spark.dataset.SparkDatasetInputFormat;
import co.cask.cdap.internal.app.runtime.spark.dataset.SparkDatasetOutputFormat;
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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.ProvisionException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.spark.deploy.SparkSubmit;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.ApplicationBundler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

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
  private Configuration sparkHConf;
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

    sparkHConf = new Configuration(hConf);

    // Create a classloader that have the context/system classloader as parent and the program classloader as child
    final ClassLoader classLoader = new CombineClassLoader(
      Objects.firstNonNull(Thread.currentThread().getContextClassLoader(), ClassLoader.getSystemClassLoader()),
      ImmutableList.of(context.getProgram().getClassLoader())
    );

    sparkHConf.setClassLoader(classLoader);

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
            sparkSubmitArgs = prepareSparkSubmitArgs(sparkSpecification, sparkHConf, programJarCopy, dependencyJar);
            LOG.info("Submitting Spark program: {} with arguments {}", context, Arrays.toString(sparkSubmitArgs));
            this.transaction = tx;
            this.cleanupTask = createCleanupTask(dependencyJar, programJarCopy);
          } catch (Throwable t) {
            Locations.deleteQuietly(dependencyJar);
            throw Throwables.propagate(t);
          }
        } catch (Throwable t) {
          Transactions.invalidateQuietly(txClient, tx);
          throw Throwables.propagate(t);
        }
      } catch (Throwable t) {
        Locations.deleteQuietly(programJarCopy);
        throw Throwables.propagate(t);
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
      Thread.currentThread().setContextClassLoader(sparkHConf.getClassLoader());
      try {
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
                                          Location dependencyJar) {
    return new String[]{"--class", SparkProgramWrapper.class.getCanonicalName(), "--jars",
      dependencyJar.toURI().getPath(), "--master", conf.get(MRConfig.FRAMEWORK_NAME), jobJarCopy.toURI().getPath(),
      sparkSpec.getMainClassName()};
  }

  /**
   * Packages all the dependencies of the Spark job
   *
   * @param context {@link BasicSparkContext} created for this job
   * @param conf    {@link Configuration} prepared for this job by {@link SparkContextConfig}
   * @return {@link Location} of the dependency jar
   * @throws IOException if failed to package the jar through
   *                     {@link ApplicationBundler#createBundle(Location, Iterable, Iterable)}
   */
  private Location buildDependencyJar(BasicSparkContext context, Configuration conf)
    throws IOException {
    ApplicationBundler appBundler = new ApplicationBundler(Lists.newArrayList("org.apache.hadoop", "org.apache.spark"),
                                                           Lists.newArrayList("org.apache.hadoop.hbase",
                                                                              "org.apache.hadoop.hive"));
    Id.Program programId = context.getProgram().getId();

    Location appFabricDependenciesJarLocation =
      locationFactory.create(String.format("%s.%s.%s.%s.%s_temp.jar",
                                           ProgramType.SPARK.name().toLowerCase(), programId.getAccountId(),
                                           programId.getApplicationId(), programId.getId(),
                                           context.getRunId().getId()));

    LOG.debug("Creating Spark Job Dependency jar: {}", appFabricDependenciesJarLocation.toURI());

    URI hConfLocation = writeHConf(context, conf);
    try {
      Set<Class<?>> classes = Sets.newHashSet();
      Set<URI> resources = Sets.newHashSet();

      classes.add(Spark.class);
      classes.add(SparkDatasetInputFormat.class);
      classes.add(SparkDatasetOutputFormat.class);
      classes.add(SparkProgramWrapper.class);
      classes.add(JavaSparkContext.class);
      classes.add(ScalaSparkContext.class);

      // We have to add this Hadoop Configuration to the dependency jar so that when the Spark job runs outside
      // CDAP it can create the BasicMapReduceContext to have access to our datasets, transactions etc.
      resources.add(hConfLocation);

      try {
        Class<?> hbaseTableUtilClass = new HBaseTableUtilFactory().get().getClass();
        classes.add(hbaseTableUtilClass);
      } catch (ProvisionException e) {
        LOG.warn("Not including HBaseTableUtil classes in submitted Job Jar since they are not available");
      }


      ClassLoader oldCLassLoader = Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader(conf.getClassLoader());
      appBundler.createBundle(appFabricDependenciesJarLocation, classes, resources);
      Thread.currentThread().setContextClassLoader(oldCLassLoader);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      deleteHConfDir(hConfLocation);
    }

    // ApplicationBundler currently packages classes, jars and resources under classes, lib,
    // resources directory. Spark expects everything to exists on top level and doesn't look for things recursively
    // under folders. So we need move everything one level up in the dependency jar.
    return updateDependencyJar(appFabricDependenciesJarLocation, context);
  }

  /**
   * Updates the dependency jar packaged by the {@link ApplicationBundler#createBundle(Location, Iterable,
   * Iterable)} by moving the things inside classes, lib, resources a level up as expected by spark.
   *
   * @param dependencyJar {@link Location} of the job jar to be updated
   * @param context       {@link BasicSparkContext} of this job
   */
  private Location updateDependencyJar(Location dependencyJar, BasicSparkContext context) throws IOException {

    final String[] prefixToStrip = {ApplicationBundler.SUBDIR_CLASSES, ApplicationBundler.SUBDIR_LIB,
      ApplicationBundler.SUBDIR_RESOURCES};

    Id.Program programId = context.getProgram().getId();

    Location updatedJar = locationFactory.create(String.format("%s.%s.%s.%s.%s.jar",
                                                               ProgramType.SPARK.name().toLowerCase(),
                                                               programId.getAccountId(),
                                                               programId.getApplicationId(), programId.getId(),
                                                               context.getRunId().getId()));

    // Creates Manifest
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.MANIFEST_VERSION, "1.0");
    JarOutputStream jarOutput = new JarOutputStream(updatedJar.getOutputStream(), manifest);

    try {
      JarInputStream jarInput = new JarInputStream(dependencyJar.getInputStream());

      try {
        JarEntry jarEntry = jarInput.getNextJarEntry();

        while (jarEntry != null) {
          boolean isDir = jarEntry.isDirectory();
          String entryName = jarEntry.getName();
          String newEntryName = entryName;

          for (String prefix : prefixToStrip) {
            if (entryName.startsWith(prefix) && !entryName.equals(prefix)) {
              newEntryName = entryName.substring(prefix.length());
            }
          }

          jarEntry = new JarEntry(newEntryName);
          jarOutput.putNextEntry(jarEntry);
          if (!isDir) {
            ByteStreams.copy(jarInput, jarOutput);
          }
          jarEntry = jarInput.getNextJarEntry();
        }
      } finally {
        jarInput.close();
        Locations.deleteQuietly(dependencyJar);
      }
    } finally {
      jarOutput.close();
    }
    return updatedJar;
  }

  /**
   * Stores the Hadoop {@link Configuration} which is then packaged with the dependency jar so that this
   * {@link Configuration} is available to Spark jobs.
   *
   * @param context {@link BasicSparkContext} created for this job
   * @param conf    {@link Configuration} of this job which has to be written to a file
   * @return {@link URI} the URI of the file to which {@link Configuration} is written
   * @throws {@link RuntimeException} if failed to get an output stream through {@link Location#getOutputStream()}
   */
  private URI writeHConf(BasicSparkContext context, Configuration conf) {
    Id.Program programId = context.getProgram().getId();
    // There can be more than one Spark job running simultaneously so store their Hadoop Configuration file under
    // different directories uniquely identified by their run id. We cannot add the run id to filename itself to
    // uniquely identify them as there is no way to access the run id in the Spark job without first loading the
    // Hadoop configuration in which the run id is stored.
    Location hConfLocation =
      locationFactory.create(String.format("%s%s/%s.%s/%s", ProgramType.SPARK.name().toLowerCase(),
                                           Location.TEMP_FILE_SUFFIX, programId.getId(), context.getRunId().getId(),
                                           SPARK_HCONF_FILENAME));

    OutputStream hConfOS = null;
    try {
      hConfOS = new BufferedOutputStream(hConfLocation.getOutputStream());
      conf.writeXml(hConfOS);
    } catch (IOException ioe) {
      LOG.error("Failed to write Hadoop Configuration file locally at {}", hConfLocation.toURI(), ioe);
      throw Throwables.propagate(ioe);
    } finally {
      Closeables.closeQuietly(hConfOS);
    }

    LOG.info("Hadoop Configuration stored at {} ", hConfLocation.toURI());
    return hConfLocation.toURI();
  }

  /**
   * Deletes the directory containing the local copy of Hadoop Configuration file created earlier.
   *
   * @param hConfLocation the {@link URI} to the Hadoop Configuration file to be deleted
   */
  private void deleteHConfDir(URI hConfLocation) {
    // get the path to the folder containing this file
    String hConfLocationFolder = hConfLocation.toString().substring(0, hConfLocation.toString().lastIndexOf("/"));
    try {
      File hConfFile = new File(new URI(hConfLocationFolder));
      FileUtils.deleteDirectory(hConfFile);
    } catch (Exception e) {
      LOG.warn("Failed to delete the local hadoop configuration");
    }
  }

  /**
   * Copies the user submitted program jar
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
                                                                   programId.getAccountId(),
                                                                   programId.getApplicationId(), programId.getId(),
                                                                   context.getRunId().getId()));

    ByteStreams.copy(Locations.newInputSupplier(jobJarLocation), Locations.newOutputSupplier(programJarCopy));
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
