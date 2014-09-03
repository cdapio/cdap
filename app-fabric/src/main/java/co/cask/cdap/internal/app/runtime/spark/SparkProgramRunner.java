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
import co.cask.cdap.api.spark.SparkContextFactory;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.app.ApplicationSpecification;
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
import co.cask.cdap.internal.app.runtime.spark.dataset.DataSetInputFormat;
import co.cask.cdap.internal.app.runtime.spark.dataset.DataSetOutputFormat;
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
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.google.inject.ProvisionException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.spark.deploy.SparkSubmit;
import org.apache.twill.api.RunId;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.ApplicationBundler;
import org.apache.twill.internal.RunIds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Enumeration;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;

/**
 * Runs {@link Spark} programs
 */
public class SparkProgramRunner implements ProgramRunner {

  public static final String SPARK_HCONF_FILENAME = "spark_hconf.xml";

  private static final Logger LOG = LoggerFactory.getLogger(SparkProgramRunner.class);
  public static final int DEFAULT_TEMP_BUFFER_SIZE = 32768;

  private final DatasetFramework datasetFramework;
  private final Configuration hConf;
  private final CConfiguration cConf;
  private SparkProgramController controller;
  private final MetricsCollectionService metricsCollectionService;
  private final ProgramServiceDiscovery serviceDiscovery;
  private final TransactionExecutorFactory txExecutorFactory;
  private final TransactionSystemClient txSystemClient;
  private final LocationFactory locationFactory;

  @Inject
  public SparkProgramRunner(DatasetFramework datasetFramework, CConfiguration cConf,
                            MetricsCollectionService metricsCollectionService,
                            ProgramServiceDiscovery serviceDiscovery, Configuration hConf,
                            TransactionExecutorFactory txExecutorFactory,
                            TransactionSystemClient txSystemClient, LocationFactory locationFactory) {
    this.hConf = hConf;
    this.datasetFramework = datasetFramework;
    this.cConf = cConf;
    this.metricsCollectionService = metricsCollectionService;
    this.serviceDiscovery = serviceDiscovery;
    this.txExecutorFactory = txExecutorFactory;
    this.locationFactory = locationFactory;
    this.txSystemClient = txSystemClient;

  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == ProgramType.SPARK, "Only Spark process type is supported.");

    SparkSpecification spec = appSpec.getSpark().get(program.getName());
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
                                                            metricsCollectionService, datasetFramework, cConf);


    try {
      Spark job = program.<Spark>getMainClass().newInstance();

      // note: this sets logging context on the thread level
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
        LOG.info("Stopping Spark Job: {}", context);
        //TODO: Add stopping logic here
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

    SparkContextConfig sparkContextConfig = new SparkContextConfig(conf);
    final Transaction tx = txSystemClient.startLong();
    // We remember tx, so that we can re-use it in Spark tasks
    sparkContextConfig.set(context, cConf, tx, jobJarCopy);

    // packaging Spark job dependency jar which includes required classes with dependencies
    final Location dependencyJar = buildDependencyJar(context, sparkContextConfig);
    LOG.info("Built Dependency Jar at {}", dependencyJar.toURI().getPath());

    final String[] sparkSubmitArgs = prepareSparkSubmitArgs(sparkSpec, conf, jobJarCopy, dependencyJar);

    new Thread() {
      @Override
      public void run() {
        boolean success = false;
        try {
          // note: this sets logging context on the thread level
          LoggingContextAccessor.setLoggingContext(context.getLoggingContext());

          LOG.info("Submitting Saprk Job: {}", context.toString());

          ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
          Thread.currentThread().setContextClassLoader(conf.getClassLoader());
          try {
            // submits job and returns immediately
            SparkSubmit.main(sparkSubmitArgs);
          } finally {
            Thread.currentThread().setContextClassLoader(oldClassLoader);
          }
        } catch (Exception e) {
          LOG.warn("Received Exception after submitting Spark Job", e);
          throw Throwables.propagate(e);
        } finally {
          // stopping controller when Spark job is finished
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
   * Prepares arguments which {@link SparkJobWrapper} is submitted to {@link SparkSubmit} to run.
   *
   * @param sparkSpec     {@link SparkSpecification} of this job
   * @param conf          {@link Configuration} of the job whose {@link MRConfig#FRAMEWORK_NAME} specifies the mode in
   *                                           which spark runs
   * @param jobJarCopy    {@link Location} copy of user program
   * @param dependencyJar {@link Location} jar containing the dependencies of this job
   * @return String[] of arguments with which {@link SparkJobWrapper} will be submitted
   */
  private String[] prepareSparkSubmitArgs(SparkSpecification sparkSpec, Configuration conf, Location jobJarCopy,
                                          Location dependencyJar) {
    return new String[]{"--class", SparkJobWrapper.class.getCanonicalName(), "--master",
      conf.get(MRConfig.FRAMEWORK_NAME), jobJarCopy.toURI().toString().split("file:", 2)[1], "--jars",
      dependencyJar.toURI().toString().split("file:", 2)[1], sparkSpec.getMainClassName()};
  }

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
            // committing long running tx: no need to commit datasets, as they were committed in external processes
            // also no need to rollback changes if commit fails, as these changes where performed by mapreduce tasks
            // NOTE: can't call afterCommit on datasets in this case: the changes were made by external processes.
            if (!txSystemClient.commit(tx)) {
              LOG.warn("Spark Job transaction failed to commit");
              success = false;
            }
          } else {
            // aborting long running tx: no need to do rollbacks, etc.
            txSystemClient.abort(tx);
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

  /**
   * Packages all the dependencies of the Spark job
   *
   * @param context            {@link BasicSparkContext} created for this job
   * @param sparkContextConfig {@link SparkContextConfig} prepared for this job through {@link Configuration}
   * @return {@link Location} of the dependency jar
   * @throws IOException if failed to package the jar through
   *                     {@link ApplicationBundler#createBundle(Location, Iterable, Iterable)}
   */
  private Location buildDependencyJar(BasicSparkContext context, SparkContextConfig sparkContextConfig)
    throws IOException {
    ApplicationBundler appBundler = new ApplicationBundler(Lists.newArrayList("org.apache.hadoop"),
                                                           Lists.newArrayList("org.apache.hadoop.hbase",
                                                                              "org.apache.hadoop.hive"));
    Id.Program programId = context.getProgram().getId();

    Location appFabricDependenciesJarLocation =
      locationFactory.create(String.format("%s.%s.%s.%s.%s.jar",
                                           ProgramType.SPARK.name().toLowerCase(), programId.getAccountId(),
                                           programId.getApplicationId(), programId.getId(),
                                           context.getRunId().getId()));

    LOG.debug("Creating Spark Job Dependency jar: {}", appFabricDependenciesJarLocation.toURI());

    URI hConfLocation = writeHConfLocally(context, sparkContextConfig.getHConf());

    Set<Class<?>> classes = Sets.newHashSet();
    Set<URI> resources = Sets.newHashSet();

    classes.add(Spark.class);
    classes.add(DataSetInputFormat.class);
    classes.add(DataSetOutputFormat.class);
    classes.add(SparkJobWrapper.class);
    classes.add(SparkContextFactory.class);
    classes.add(JavaSparkContext.class);
    classes.add(ScalaSparkContext.class);

    /**
     * We have to add this Hadoop {@link Configuration} to the dependency jar so thay when the Spark job runs outside
     * CDAP it can create the {@link BasicMapReduceContext} to have access to our datasets, transactions etc.
     */
    resources.add(hConfLocation);

    try {
      Class<?> hbaseTableUtilClass = new HBaseTableUtilFactory().get().getClass();
      classes.add(hbaseTableUtilClass);
    } catch (ProvisionException e) {
      LOG.warn("Not including HBaseTableUtil classes in submitted Job Jar since they are not available");
    }

    ClassLoader oldCLassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(sparkContextConfig.getHConf().getClassLoader());
    appBundler.createBundle(appFabricDependenciesJarLocation, classes, resources);
    Thread.currentThread().setContextClassLoader(oldCLassLoader);

    deleteLocalHConf(hConfLocation);

    /**
     * {@link ApplicationBundler} currently packages classes, jars and resources under classes, lib,
     * resources directory. Spark expects everything to exists on top level and doesn't look for things recursively
     * under folders. So we need move everything one level up in the dependency jar.
     * TODO: This is inefficient. ApplicationBundler should be changed to support hierarchy less packaging.
     */
    updateDependencyJar(appFabricDependenciesJarLocation);

    return appFabricDependenciesJarLocation;
  }

  /**
   * Deletes the local copy of Hadoop Configuration file created earlier
   * jar.
   *
   * @param hConfLocation the {@link URI} to the Hadoop Configuration file to be deleted
   */
  private void deleteLocalHConf(URI hConfLocation) {
    // get the path to the folder containing this file
    String hConfLocationFolder = hConfLocation.toString().split(("/" + SPARK_HCONF_FILENAME), 2)[0];
    File hConfFile = null;
    try {
      hConfFile = new File(new URI(hConfLocationFolder));
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    if (hConfFile.exists()) {
      try {
        FileUtils.deleteDirectory(hConfFile);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Copies the user submitted program jar
   *
   * @param jobJarLocation {link Location} of the user's job
   * @param context        {@link BasicSparkContext} context of this job
   * @return {@link Location} where the programn jar was copied
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
   * Updates the dependency jar packaged by the {@link ApplicationBundler#createBundle(Location, Iterable,
   * Iterable)} by moving the things inside classes, lib, resources a level up as expected by spark.
   *
   * @param jobJar {@link Location} of the job jar to be updated
   */
  private void updateDependencyJar(Location jobJar) {

    JarFile jarFile = null;
    File srcJarFile = null;
    JarOutputStream tmpJarOutputStream = null;
    boolean jarUpdated = false;
    File tmpJobJarFile = null;

    try {
      srcJarFile = new File(jobJar.toURI());
      jarFile = new JarFile(srcJarFile);
      tmpJobJarFile = File.createTempFile("tmpJobJarFile", ".tmp");

      try {
        tmpJarOutputStream = new JarOutputStream(new FileOutputStream(tmpJobJarFile));
        Enumeration jarEntries = jarFile.entries();
        byte[] buffer = new byte[DEFAULT_TEMP_BUFFER_SIZE];

        while (jarEntries.hasMoreElements()) {
          JarEntry entry = (JarEntry) jarEntries.nextElement();
          // skip the folder classes, lib and resources
          if (entry.getName().equals(ApplicationBundler.SUBDIR_CLASSES) || entry.getName().equals(ApplicationBundler
                                                                                                    .SUBDIR_LIB) ||
            entry.getName().equals(ApplicationBundler.SUBDIR_RESOURCES)) {
            continue;
          }
          // if this jar entry is under classes, lib or resources then move them up
          if (entry.getName().startsWith(ApplicationBundler.SUBDIR_CLASSES) || entry.getName().startsWith
            (ApplicationBundler.SUBDIR_LIB) || entry.getName().startsWith(ApplicationBundler.SUBDIR_RESOURCES)) {
            if (entry.getName().startsWith(ApplicationBundler.SUBDIR_CLASSES)) {
              tmpJarOutputStream.putNextEntry(new JarEntry(entry.getName().split(ApplicationBundler.SUBDIR_CLASSES,
                                                                                 2)[1]));
            } else if (entry.getName().startsWith(ApplicationBundler.SUBDIR_LIB)) {

              tmpJarOutputStream.putNextEntry(new JarEntry(entry.getName().split(ApplicationBundler.SUBDIR_LIB,
                                                                                 2)[1]));
            } else {

              tmpJarOutputStream.putNextEntry(new JarEntry(entry.getName().split(ApplicationBundler.SUBDIR_RESOURCES,
                                                                                 2)[1]));
            }
          } else {
            tmpJarOutputStream.putNextEntry(entry);
          }
          InputStream entryInputStream = jarFile.getInputStream(entry);
          int bytesRead;
          while ((bytesRead = entryInputStream.read(buffer)) != -1) {
            tmpJarOutputStream.write(buffer, 0, bytesRead);
          }
        }
        jarUpdated = true;
      } catch (Exception e) {
        LOG.warn("Failed to create the new job jar without folder hierarchy", e);
        tmpJarOutputStream.putNextEntry(new JarEntry("stub"));
      } finally {
        tmpJarOutputStream.close();
      }
    } catch (IOException e) {
      LOG.warn("Failed to to get the jar file from the location {}", jobJar.toURI().toString(), e);
    } finally {
      try {
        if (jarFile != null) {
          jarFile.close();
        }
      } catch (IOException e) {
        LOG.warn("Failed to close the source jar", e);
      }
      if (!jarUpdated) {
        tmpJobJarFile.delete();
      }
    }
    if (jarUpdated) {
      srcJarFile.delete();
      tmpJobJarFile.renameTo(srcJarFile);
    }
  }

  /**
   * Stores the Hadoop {@link Configuration} locally which is then packaged with the dependency jar so that this
   * {@link Configuration} is available to Spark jobs.
   *
   * @param context {@link BasicSparkContext} created for this job
   * @param conf    {@link Configuration} of this job which has to be written to a file
   * @return {@link URI} the URI of the file to which {@link Configuration} is written
   * @throws IOException if failed to get an output stream through {@link Location#getOutputStream()}
   */
  private URI writeHConfLocally(BasicSparkContext context, Configuration conf) throws IOException {
    Id.Program programId = context.getProgram().getId();
    // There can be more than one Spark job running simultaneously so store their Hadoop Configuration file under
    // different directories uniquely identified by their run id. We cannot add the run id to filename itself to
    // uniquely identify them as there is no way to access the run id in the Spark job without first loading the
    // Hadoop configuration in which the run id is stored.
    Location hConfLocation =
      locationFactory.create(String.format("%s%s%s%s.%s%s%s", ProgramType.SPARK.getPrettyName(),
                                           Location.TEMP_FILE_SUFFIX, "/", programId.getId(),
                                           context.getRunId().getId(), "/", SPARK_HCONF_FILENAME));

    OutputStream hConfOS = new BufferedOutputStream(hConfLocation.getOutputStream());
    conf.writeXml(hConfOS);
    hConfOS.flush();
    hConfOS.close();
    LOG.info("Hadoop Configuration stored at {} ", hConfLocation.toURI());
    return hConfLocation.toURI();
  }
}
