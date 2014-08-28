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
import java.util.Enumeration;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;

/**
 * Runs {@link Spark} programs
 */
public class SparkProgramRunner implements ProgramRunner {

  public static final String SPARK_HCONF_FILENAME = "spark_hConf.xml";

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
    RunId runId = arguments.hasOption(ProgramOptionConstants.RUN_ID)
      ? RunIds.fromString(arguments.getOption(ProgramOptionConstants.RUN_ID))
      : RunIds.generate();

    long logicalStartTime = arguments.hasOption(ProgramOptionConstants.LOGICAL_START_TIME)
      ? Long.parseLong(arguments
                         .getOption(ProgramOptionConstants.LOGICAL_START_TIME))
      : System.currentTimeMillis();

    String workflowBatch = arguments.getOption(ProgramOptionConstants.WORKFLOW_BATCH);

//    DataSetInstantiator dataSetInstantiator = new DataSetInstantiator(datasetFramework,
//                                                                      cConf, program.getClassLoader());
//    Map<String, DatasetCreationSpec> datasetSpecs = program.getSpecification().getDatasets();
//    Map<String, Closeable> dataSets = DataSets.createDataSets(dataSetInstantiator, datasetSpecs.keySet());

    final BasicSparkContext context = new BasicSparkContext(program, runId, options.getUserArguments(),
                                                            program.getSpecification().getDatasets().keySet(), spec,
                                                            logicalStartTime, workflowBatch, serviceDiscovery,
                                                            metricsCollectionService,
                                                            datasetFramework, cConf);


    try {
      Spark job = program.<Spark>getMainClass().newInstance();

//      Reflections.visit(job, TypeToken.of(job.getClass()),
//                        new PropertyFieldSetter(context.getSpecification().getProperties()),
//                        new DataSetFieldSetter(context));

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

    // adding listener which stops mapreduce job when controller stops.
    controller.addListener(new AbstractListener() {
      @Override
      public void stopping() {
        LOG.info("Stopping Spark Job: {}", context);
        //TODO: Add stopping logic here
      }
    }, MoreExecutors.sameThreadExecutor());
    return controller;
  }

  private void submit(final Spark job, SparkSpecification sparkSpec, Location jobJarLocation,
                      final BasicSparkContext context) throws Exception {

    final Configuration conf = new Configuration(hConf);
    //TODO: This should be dynamically determined for Distributed support
    conf.set(MRConfig.FRAMEWORK_NAME, "local");


    // Create a classloader that have the context/system classloader as parent and the program classloader as child
    final ClassLoader classLoader = new CombineClassLoader(
      Objects.firstNonNull(Thread.currentThread().getContextClassLoader(), ClassLoader.getSystemClassLoader()),
      ImmutableList.of(context.getProgram().getClassLoader())
    );

    conf.setClassLoader(classLoader);

    // additional spark job initialization at run-time
    beforeSubmit(job, context);


    final Location programJarCopy = createJobJarTempCopy(jobJarLocation, context);
    LOG.info("Copied Program Jar to {}, source: {}", programJarCopy.toURI().getPath(),
             jobJarLocation.toURI().toString());

    SparkContextConfig sparkContextConfig = new SparkContextConfig(conf);
    final Transaction tx = txSystemClient.startLong();
    // We remember tx, so that we can re-use it in mapreduce tasks
    sparkContextConfig.set(context, cConf, tx, programJarCopy);

    // packaging job jar which includes continuuity classes with dependencies
    // NOTE: user's jar is added to classpath separately to leave the flexibility in future to create and use separate
    //       classloader when executing user code. We need to submit a copy of the program jar because
    //       in distributed mode this returns program path on HDFS, not localized, which may cause race conditions
    //       if we allow deploying new program while existing is running. To prevent races we submit a temp copy

    final Location jobJar = buildJobJar(context, sparkContextConfig);
    LOG.info("Built Spark Job Jar at {}", jobJar.toURI().getPath());

    // The above created jar is packaged insided folders. Move all the classes, libs and resources to top level as
    // spark does not recursively checks inside folder
    updateJobJar(jobJar);


//    jobConf.setJar(jobJar.toURI().toString());
//    jobConf.addFileToClassPath(new Path(programJarCopy.toURI()));

    String ab = jobJar.toURI().toString().split("file:", 2)[1];
    String cd = programJarCopy.toURI().toString().split("file:", 2)[1];

    System.out.println("prgram jar: " + cd);
    System.out.println("job jar: " + ab);

    final String[] userArgs = {"--class", "co.cask.cdap.internal.app.runtime.spark.SparkJobWrapper", "--master",
      "local", cd, "--jars", ab, sparkSpec.getMainClassName()};

    new Thread() {
      @Override
      public void run() {
        boolean success = false;
        try {
          // note: this sets logging context on the thread level
          LoggingContextAccessor.setLoggingContext(context.getLoggingContext());

          LOG.info("Submitting MapReduce Job: {}", context.toString());

          ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
          Thread.currentThread().setContextClassLoader(conf.getClassLoader());
          try {
            // submits job and returns immediately
            SparkSubmit.main(userArgs);
          } finally {
            Thread.currentThread().setContextClassLoader(oldClassLoader);
          }

          //MapReduceMetricsWriter metricsWriter = new MapReduceMetricsWriter(jobConf, context);

//            // until job is complete report stats
//            while (!jobConf.isComplete()) {
//              metricsWriter.reportStats();
//
//              // we report to metrics backend every second, so 1 sec is enough here. That's mapreduce job anyways (not
//              // short) ;)
//              TimeUnit.MILLISECONDS.sleep(1000);
//            }

//            LOG.info("MapReduce Job is complete, status: {}, success: {}, job: {}" + jobConf.getStatus(), success,
//                     context.toString());
//            // NOTE: we want to report the final stats (they may change since last report and before job completed)
//            metricsWriter.reportStats();
//            // If we don't sleep, the final stats may not get sent before shutdown.
          //TimeUnit.SECONDS.sleep(2L);

          //success = jobConf.isSuccessful();
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    }.start();

  }

  private void updateJobJar(Location jobJar) {

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
        while (jarEntries.hasMoreElements()) {
          JarEntry entry = (JarEntry) jarEntries.nextElement();
          if (entry.getName().equals(ApplicationBundler.SUBDIR_CLASSES) || entry.getName().equals(ApplicationBundler
                                                                                                    .SUBDIR_LIB) ||
            entry.getName().equals(ApplicationBundler.SUBDIR_RESOURCES)) {
            continue; // skip the folder classes, lib and resources
          }
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
          byte[] buffer = new byte[1024];
          int bytesRead = 0;
          while ((bytesRead = entryInputStream.read(buffer)) != -1) {
            tmpJarOutputStream.write(buffer, 0, bytesRead);
          }
        }
        jarUpdated = true;
      } catch (Exception e) {
        LOG.warn("Failed to create the new job jar without folder hierarcy", e);
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
        e.printStackTrace();
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

  private void onFinish(final Spark job,
                        final BasicSparkContext context,
                        final boolean succeeded)
    throws TransactionFailureException, InterruptedException {
    TransactionExecutor txExecutor =
      txExecutorFactory.createExecutor(context.getDatasetInstantiator().getTransactionAware());
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        job.onFinish(succeeded, context);
      }
    });
  }

  private URI storeHConfLocally(BasicSparkContext context, Configuration conf) throws IOException {
    Id.Program programId = context.getProgram().getId();
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

  private void beforeSubmit(final Spark job,
                            final BasicSparkContext context)
    throws TransactionFailureException, InterruptedException {

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


  private Location buildJobJar(BasicSparkContext context, SparkContextConfig sparkContextConfig) throws IOException {
    ApplicationBundler appBundler = new ApplicationBundler(Lists.newArrayList("org.apache.hadoop"),
                                                           Lists.newArrayList("org.apache.hadoop.hbase",
                                                                              "org.apache.hadoop.hive"));
    Id.Program programId = context.getProgram().getId();

    Location appFabricDependenciesJarLocation =
      locationFactory.create(String.format("%s.%s.%s.%s.%s.jar",
                                           ProgramType.SPARK.name().toLowerCase(),
                                           programId.getAccountId(), programId.getApplicationId(),
                                           programId.getId(), context.getRunId().getId()));

    URI confLocation = storeHConfLocally(context, sparkContextConfig.getHConf());

    LOG.debug("Creating Job jar: {}", appFabricDependenciesJarLocation.toURI());

    Set<Class<?>> classes = Sets.newHashSet();
    Set<URI> resources = Sets.newHashSet();

    classes.add(Spark.class);
    classes.add(DataSetInputFormat.class);
    classes.add(DataSetOutputFormat.class);
    classes.add(SparkJobWrapper.class);
    classes.add(SparkContextFactory.class);
    classes.add(JavaSparkContext.class);
    classes.add(ScalaSparkContext.class);

    resources.add(confLocation);


//    Job jobConf = context.getHadoopJob();
//    try {
//      Class<? extends InputFormat<?, ?>> inputFormatClass = jobConf.getInputFormatClass();
//      LOG.info("InputFormat class: {} {}", inputFormatClass, inputFormatClass.getClassLoader());
//      classes.add(inputFormatClass);
//    } catch (Throwable t) {
//      LOG.info("InputFormat class not found: {}", t.getMessage(), t);
//      // Ignore
//    }
//    try {
//      Class<? extends OutputFormat<?, ?>> outputFormatClass = jobConf.getOutputFormatClass();
//      LOG.info("OutputFormat class: {} {}", outputFormatClass, outputFormatClass.getClassLoader());
//      classes.add(outputFormatClass);
//    } catch (Throwable t) {
//      LOG.info("OutputFormat class not found: {}", t.getMessage(), t);
//      // Ignore
//    }

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
    deleteLocalHConf(confLocation);

    return appFabricDependenciesJarLocation;
  }

  private void deleteLocalHConf(URI confLocation) {
    File hConfFile = new File(confLocation);
    if (hConfFile.exists()) {
      hConfFile.delete();
    }
  }


  private Location createJobJarTempCopy(Location jobJarLocation, BasicSparkContext context) throws IOException {

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
}
