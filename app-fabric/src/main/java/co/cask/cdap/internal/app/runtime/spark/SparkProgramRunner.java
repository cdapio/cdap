/*
 * Copyright 2014 Cask Data, Inc.
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
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.lang.CombineClassLoader;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramServiceDiscovery;
import co.cask.cdap.internal.app.runtime.batch.BasicMapReduceContext;
import co.cask.cdap.internal.app.runtime.spark.dataset.SparkDatasetInputFormat;
import co.cask.cdap.internal.app.runtime.spark.dataset.SparkDatasetOutputFormat;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
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
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.google.inject.ProvisionException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.spark.deploy.SparkSubmit;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.ApplicationBundler;
import org.apache.twill.internal.RunIds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * Runs {@link Spark} programs
 */
public class SparkProgramRunner implements ProgramRunner {

  public static final String SPARK_HCONF_FILENAME = "spark_hconf.xml";
  private static final Logger LOG = LoggerFactory.getLogger(SparkProgramRunner.class);

  private final DatasetFramework datasetFramework;
  private final Configuration hConf;
  private final CConfiguration cConf;
  private SparkProgramController controller;
  private final MetricsCollectionService metricsCollectionService;
  private final ProgramServiceDiscovery serviceDiscovery;
  private final TransactionExecutorFactory txExecutorFactory;
  private final TransactionSystemClient txSystemClient;
  private final LocationFactory locationFactory;
  private final DiscoveryServiceClient discoveryServiceClient;

  @Inject
  public SparkProgramRunner(DatasetFramework datasetFramework, CConfiguration cConf,
                            MetricsCollectionService metricsCollectionService,
                            ProgramServiceDiscovery serviceDiscovery, Configuration hConf,
                            TransactionExecutorFactory txExecutorFactory,
                            TransactionSystemClient txSystemClient, LocationFactory locationFactory,
                            DiscoveryServiceClient discoveryServiceClient) {
    this.hConf = hConf;
    this.datasetFramework = datasetFramework;
    this.cConf = cConf;
    this.metricsCollectionService = metricsCollectionService;
    this.serviceDiscovery = serviceDiscovery;
    this.txExecutorFactory = txExecutorFactory;
    this.locationFactory = locationFactory;
    this.txSystemClient = txSystemClient;
    this.discoveryServiceClient = discoveryServiceClient;

  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    // Extract and verify parameters
    final ApplicationSpecification appSpec = program.getSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == ProgramType.SPARK, "Only Spark process type is supported.");

    final SparkSpecification spec = appSpec.getSpark().get(program.getName());
    Preconditions.checkNotNull(spec, "Missing SparkSpecification for %s", program.getName());

    // Optionally get runId. If the spark started by other program (e.g. Workflow), it inherit the runId.
    Arguments arguments = options.getArguments();
    RunId runId = arguments.hasOption(ProgramOptionConstants.RUN_ID) ? RunIds.fromString(arguments.getOption
      (ProgramOptionConstants.RUN_ID)) : RunIds.generate();

    long logicalStartTime = arguments.hasOption(ProgramOptionConstants.LOGICAL_START_TIME)
      ? Long.parseLong(arguments.getOption(ProgramOptionConstants.LOGICAL_START_TIME)) : System.currentTimeMillis();

    String workflowBatch = arguments.getOption(ProgramOptionConstants.WORKFLOW_BATCH);

    final BasicSparkContext context = new BasicSparkContext(program, runId, options.getUserArguments(),
                                                            program.getSpecification().getDatasets().keySet(), spec,
                                                            logicalStartTime, workflowBatch, serviceDiscovery,
                                                            metricsCollectionService, datasetFramework, cConf,
                                                            discoveryServiceClient);


    try {
      Spark job = program.<Spark>getMainClass().newInstance();

      LoggingContextAccessor.setLoggingContext(context.getLoggingContext());

      controller = new SparkProgramController(context);

      LOG.info("Starting Spark Job: {}", context.toString());
      submit(job, spec, program.getJarLocation(), context);

    } catch (Throwable e) {
      // failed before job even started - release all resources of the context
      context.close();
      throw Throwables.propagate(e);
    }

    // adding listener which stops spark job when controller stops.
    controller.addListener(new AbstractListener() {
      @Override
      public void stopping() {
        // TODO: This does not work as Spark goes into deadlock while closing the context in local mode
        // Jira: REACTOR-951
        LOG.info("Stopping Spark Job: {}", context);
        try {
          if (SparkProgramWrapper.isSparkProgramRunning()) {
            SparkProgramWrapper.stopSparkProgram();
          }
        } catch (Exception e) {
          LOG.error("Failed to stop Spark job {}", spec.getName(), e);
          throw Throwables.propagate(e);
        }
      }
    }, MoreExecutors.sameThreadExecutor());
    return controller;
  }

  private void submit(final Spark job, SparkSpecification sparkSpec, Location programJarLocation,
                      final BasicSparkContext context) throws Exception {

    final Configuration conf = new Configuration(hConf);

    // Create a classloader that have the context/system classloader as parent and the program classloader as child
    final ClassLoader classLoader = new CombineClassLoader(
      Objects.firstNonNull(Thread.currentThread().getContextClassLoader(), ClassLoader.getSystemClassLoader()),
      ImmutableList.of(context.getProgram().getClassLoader())
    );

    conf.setClassLoader(classLoader);

    // additional spark job initialization at run-time
    beforeSubmit(job, context);

    final Location jobJarCopy = copyProgramJar(programJarLocation, context);
    LOG.info("Copied Program Jar to {}, source: {}", jobJarCopy.toURI().getPath(),
             programJarLocation.toURI().toString());


    final Transaction tx = txSystemClient.startLong();
    // We remember tx, so that we can re-use it in Spark tasks
    SparkContextConfig.set(conf, context, cConf, tx, jobJarCopy);

    // packaging Spark job dependency jar which includes required classes with dependencies
    final Location dependencyJar = buildDependencyJar(context, SparkContextConfig.getHConf());
    LOG.info("Built Dependency Jar at {}", dependencyJar.toURI().getPath());

    final String[] sparkSubmitArgs = prepareSparkSubmitArgs(sparkSpec, conf, jobJarCopy, dependencyJar);

    new Thread() {
      @Override
      public void run() {
        boolean success = false;
        try {
          LoggingContextAccessor.setLoggingContext(context.getLoggingContext());

          LOG.info("Submitting Spark Job: {}", context.toString());

          ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
          Thread.currentThread().setContextClassLoader(conf.getClassLoader());
          try {
            SparkProgramWrapper.setSparkProgramRunning(true);
            SparkSubmit.main(sparkSubmitArgs);
          } catch (Exception e) {
            LOG.error("Received Exception after submitting Spark Job", e);
          } finally {
            // job completed so update running status and get the success status
            success = SparkProgramWrapper.isSparkProgramSuccessful();
            SparkProgramWrapper.setSparkProgramRunning(false);
            Thread.currentThread().setContextClassLoader(oldClassLoader);
          }
        } catch (Exception e) {
          throw Throwables.propagate(e);
        } finally {
          stopController(success, context, job, tx);
          try {
            dependencyJar.delete();
          } catch (IOException e) {
            LOG.warn("Failed to delete Spark Job Dependency Jar: {}", dependencyJar.toURI());
          }
          try {
            jobJarCopy.delete();
          } catch (IOException e) {
            LOG.warn("Failed to delete Spark Job Jar: {}", jobJarCopy.toURI());
          }
        }
      }
    }.start();
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
    return new String[]{"--class", SparkProgramWrapper.class.getCanonicalName(), "--master",
      conf.get(MRConfig.FRAMEWORK_NAME), jobJarCopy.toURI().getPath(), "--jars", dependencyJar.toURI().getPath(),
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
    ApplicationBundler appBundler = new ApplicationBundler(Lists.newArrayList("org.apache.hadoop"),
                                                           Lists.newArrayList("org.apache.hadoop.hbase",
                                                                              "org.apache.hadoop.hive"));
    Id.Program programId = context.getProgram().getId();

    Location appFabricDependenciesJarLocation =
      locationFactory.create(String.format("%s.%s.%s.%s.%s_temp.jar",
                                           ProgramType.SPARK.name().toLowerCase(), programId.getAccountId(),
                                           programId.getApplicationId(), programId.getId(),
                                           context.getRunId().getId()));

    LOG.debug("Creating Spark Job Dependency jar: {}", appFabricDependenciesJarLocation.toURI());

    URI hConfLocation = writeHConfLocally(context, conf);

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

    try {
      ClassLoader oldCLassLoader = Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader(conf.getClassLoader());
      appBundler.createBundle(appFabricDependenciesJarLocation, classes, resources);
      Thread.currentThread().setContextClassLoader(oldCLassLoader);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      deleteLocalHConf(hConfLocation);
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
        dependencyJar.delete();
      }
    } finally {
      jarOutput.close();
    }
    return updatedJar;
  }

  /**
   * Stores the Hadoop {@link Configuration} locally which is then packaged with the dependency jar so that this
   * {@link Configuration} is available to Spark jobs.
   *
   * @param context {@link BasicSparkContext} created for this job
   * @param conf    {@link Configuration} of this job which has to be written to a file
   * @return {@link URI} the URI of the file to which {@link Configuration} is written
   * @throws {@link RuntimeException} if failed to get an output stream through {@link Location#getOutputStream()}
   */
  private URI writeHConfLocally(BasicSparkContext context, Configuration conf) {
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
      hConfOS.flush();
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
   * Deletes the local copy of Hadoop Configuration file created earlier.
   *
   * @param hConfLocation the {@link URI} to the Hadoop Configuration file to be deleted
   */
  private void deleteLocalHConf(URI hConfLocation) {
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

  /**
   * Called before submitting spark job
   *
   * @param job     the {@link Spark} job
   * @param context {@link BasicMapReduceContext} of this job
   * @throws TransactionFailureException
   * @throws InterruptedException
   */
  private void beforeSubmit(final Spark job, final BasicSparkContext context) throws TransactionFailureException,
    InterruptedException {

    TransactionExecutor txExecutor = txExecutorFactory.createExecutor(context.getDatasetInstantiator()
                                                                        .getTransactionAware());

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

  /**
   * Called after the spark job finishes
   *
   * @param job       the {@link Spark} job
   * @param context   {@link BasicMapReduceContext} of this job
   * @param succeeded boolean of job status
   * @throws TransactionFailureException
   * @throws InterruptedException
   */
  private void onFinish(final Spark job, final BasicSparkContext context, final boolean succeeded) throws
    TransactionFailureException, InterruptedException {
    TransactionExecutor txExecutor = txExecutorFactory.createExecutor(context.getDatasetInstantiator()
                                                                        .getTransactionAware());
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        job.onFinish(succeeded, context);
      }
    });
  }

  /**
   * Stops the controller depending upon the job success status
   *
   * @param success boolean for job status
   * @param context {@link BasicSparkContext} of this job
   * @param job     {@link Spark} job
   * @param tx      {@link Transaction} the transaction for this job
   */
  private void stopController(boolean success, BasicSparkContext context, Spark job, Transaction tx) {
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
            if (!txSystemClient.commit(tx)) {
              LOG.warn("Spark Job transaction failed to commit");
              success = false;
            }
          } else {
            // invalidate the transaction as spark might have written to datasets too
            txSystemClient.invalidate(tx.getWritePointer());
          }
        } finally {
          // whatever happens we want to call this
          onFinish(job, context, success);
        }
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    } finally {
      // release all resources, datasets, etc. of the context
      context.close();
    }
  }
}
