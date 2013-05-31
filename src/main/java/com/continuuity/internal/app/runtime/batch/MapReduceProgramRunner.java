package com.continuuity.internal.app.runtime.batch;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.batch.MapReduce;
import com.continuuity.api.batch.MapReduceSpecification;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.app.runtime.RunId;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.common.logging.common.LogWriter;
import com.continuuity.common.logging.logback.CAppender;
import com.continuuity.data.DataFabric;
import com.continuuity.data.DataFabricImpl;
import com.continuuity.data.dataset.DataSetInstantiator;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.SmartTransactionAgent;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.data.operation.executor.TransactionProxy;
import com.continuuity.internal.app.runtime.AbstractListener;
import com.continuuity.internal.app.runtime.AbstractProgramController;
import com.continuuity.internal.app.runtime.DataSets;
import com.continuuity.internal.app.runtime.batch.dataset.DataSetInputFormat;
import com.continuuity.internal.app.runtime.batch.dataset.DataSetOutputFormat;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.internal.ApplicationBundler;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Set;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

/**
 * Runs {@link com.continuuity.api.batch.MapReduce} programs
 */
public class MapReduceProgramRunner implements ProgramRunner {
  private static final Logger LOG = LoggerFactory.getLogger(MapReduceProgramRunner.class);

  private final OperationExecutor opex;

  private final CConfiguration cConf;
  private final Configuration hConf;

  private Job jobConf;
  private MapReduceProgramController controller;
  private TransactionAgent txAgent;

  @Inject
  public MapReduceProgramRunner(CConfiguration cConf, Configuration hConf,
                                OperationExecutor opex) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.opex = opex;
  }

  @Inject (optional = true)
  void setLogWriter(@Nullable LogWriter logWriter) {
    if (logWriter != null) {
      CAppender.logWriter = logWriter;
    }
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    Type processorType = program.getProcessorType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == Type.MAPREDUCE, "Only MAPREDUCE process type is supported.");

    MapReduceSpecification spec = appSpec.getMapReduces().get(program.getProgramName());
    Preconditions.checkNotNull(spec, "Missing MapReduceSpecification for %s", program.getProgramName());

    OperationContext opexContext = new OperationContext(program.getAccountId(), program.getApplicationId());

    // Starting long-running transaction that we will also use in mapreduce tasks
    Transaction tx;
    try {
      tx = opex.startTransaction(opexContext, false);
      txAgent = new SmartTransactionAgent(opex, opexContext, tx);
      txAgent.start();
    } catch (OperationException e) {
      LOG.error("Failed to start transaction for mapreduce job: " + program.getProgramName());
      throw Throwables.propagate(e);
    }

    TransactionProxy transactionProxy = new TransactionProxy();
    transactionProxy.setTransactionAgent(txAgent);

    DataFabric dataFabric = new DataFabricImpl(opex, opexContext);
    DataSetInstantiator dataSetContext =
      new DataSetInstantiator(dataFabric, transactionProxy, program.getClassLoader());
    dataSetContext.setDataSets(Lists.newArrayList(program.getSpecification().getDataSets().values()));

    try {
      RunId runId = RunId.generate();
      final BasicMapReduceContext context =
        new BasicMapReduceContext(program, runId, txAgent,
                                  DataSets.createDataSets(dataSetContext, spec.getDataSets()), spec);

      MapReduce job = (MapReduce) program.getMainClass().newInstance();
      context.injectFields(job);

      // note: this sets logging context on the thread level
      LoggingContextAccessor.setLoggingContext(context.getLoggingContext());

      controller = new MapReduceProgramController(context);

      LOG.info("Starting MapReduce job: " + context.toString());
      submit(job, program.getProgramJarLocation(), context, tx);

      // adding listener which stops mapreduce job when controller stops.
      controller.addListener(new AbstractListener() {
        @Override
        public void stopping() {
          LOG.info("Stopping mapreduce job: " + context);
          try {
            if (!jobConf.isComplete()) {
              jobConf.killJob();
            }
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
          LOG.info("Mapreduce job stopped: " + context);
        }
      }, MoreExecutors.sameThreadExecutor());

      return controller;

    } catch (Throwable e) {
      try {
        transactionProxy.getTransactionAgent().abort();
      } catch (OperationException ex) {
        throw Throwables.propagate(ex);
      }
      LOG.error("Failed to run mapreduce job: " + program.getProgramName(), e);
      throw Throwables.propagate(e);
    }
  }

  private void submit(final MapReduce job, Location jobJarLocation, final BasicMapReduceContext context, Transaction tx)
    throws Exception {
    jobConf = Job.getInstance(hConf);
    context.setJob(jobConf);
    // additional mapreduce job initialization at run-time
    job.beforeSubmit(context);

    // we do flush to make operations executed in beforeSubmit() visible in mapreduce tasks (which may run in a
    // different JVM)
    context.flushOperations();

    // replace user's Mapper & Reducer's with our wrappers in job config
    wrapMapperClassIfNeeded(jobConf);
    wrapReducerClassIfNeeded(jobConf);

    // set input/output datasets info
    setInputDataSetIfNeeded(jobConf, context);
    setOutputDataSetIfNeeded(jobConf, context);

    MapReduceContextProvider contextProvider = new MapReduceContextProvider(jobConf);
    // apart from everything we also remember tx, so that we can re-use it in mapreduce tasks
    contextProvider.set(context, cConf, tx);

    // packaging job jar with dependencies (including app-fabric dependencies)
    final Location jobJar = buildJobJar(job, jobJarLocation);

    jobConf.setJar(jobJar.toURI().toString());
    // This is needed for having the program jar file available in MR classpath
    jobConf.addFileToClassPath(new Path(jobJar.toURI()));
    jobConf.getConfiguration().setClassLoader(context.getProgram().getClassLoader());

    new Thread() {
      @Override
      public void run() {
        boolean success = false;
        try {
          // note: this sets logging context on the thread level
          LoggingContextAccessor.setLoggingContext(context.getLoggingContext());
          try {
            LOG.info("Submitting mapreduce job {}", context.toString());
            success = jobConf.waitForCompletion(true);
          } catch (InterruptedException e) {
            // nothing we can do now: we simply stopped watching for job completion...
            throw Throwables.propagate(e);
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }

          job.onFinish(success, context);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        } finally {
          // stopping controller when mapreduce job is finished
          // (also that should finish transaction, but that might change after integration with "long running txs")
          stopController(success);
          try {
            jobJar.delete();
          } catch (IOException e) {
            LOG.warn("Failed to delete temp mr job jar: " + jobJar.toURI());
            // failure should not affect other stuff
          }
        }
      }
    }.start();
  }

  private void wrapMapperClassIfNeeded(Job jobConf) throws ClassNotFoundException {
    // NOTE: we don't use jobConf.getMapperClass() as we cannot (and don't want to) access user class here
    String mapClass = jobConf.getConfiguration().get(MRJobConfig.MAP_CLASS_ATTR);
    if (mapClass != null) {
      jobConf.getConfiguration().set(MapperWrapper.ATTR_MAPPER_CLASS, mapClass);
      // yes, it is a subclass of Mapper
      @SuppressWarnings("unchecked")
      Class<? extends Mapper> wrapperClass = MapperWrapper.class;
      jobConf.setMapperClass(wrapperClass);
    }
  }

  private void wrapReducerClassIfNeeded(Job jobConf) throws ClassNotFoundException {
    // NOTE: we don't use jobConf.getReducerClass() as we cannot (and don't want to) access user class here
    String reducerClass = jobConf.getConfiguration().get(MRJobConfig.REDUCE_CLASS_ATTR);
    if (reducerClass != null) {
      jobConf.getConfiguration().set(ReducerWrapper.ATTR_REDUCER_CLASS, reducerClass);
      // yes, it is a subclass of Reducer
      @SuppressWarnings("unchecked")
      Class<? extends Reducer> wrapperClass = ReducerWrapper.class;
      jobConf.setReducerClass(wrapperClass);
    }
  }

  private void stopController(boolean success) {
    controller.stop();
    try {
      if (success) {
        txAgent.finish();
      } else {
        txAgent.abort();
      }
    } catch (OperationException e) {
      throw Throwables.propagate(e);
    }
  }

  private DataSet setOutputDataSetIfNeeded(Job jobConf, BasicMapReduceContext mapReduceContext) {
    DataSet outputDataset = null;
    // whatever was set into mapReduceContext e.g. during beforeSubmit(..) takes precedence
    if (mapReduceContext.getOutputDataset() != null) {
      outputDataset = (DataSet) mapReduceContext.getOutputDataset();
    } else {
      // trying to init output dataset from spec
      String outputDataSetName = mapReduceContext.getSpecification().getOutputDataSet();
      if (outputDataSetName != null) {
        // We checked on validation phase that it implements BatchWritable
        outputDataset = mapReduceContext.getDataSet(outputDataSetName);
        mapReduceContext.setOutput((BatchWritable) outputDataset);
      }
    }

    if (outputDataset != null) {
      LOG.debug("Using dataset {} as output for mapreduce job", outputDataset.getName());
      DataSetOutputFormat.setOutput(jobConf, outputDataset);
    }
    return outputDataset;
  }

  private DataSet setInputDataSetIfNeeded(Job jobConf, BasicMapReduceContext mapReduceContext)
    throws OperationException {
    DataSet inputDataset = null;
    // whatever was set into mapReduceJob e.g. during beforeSubmit(..) takes precedence
    if (mapReduceContext.getInputDataset() != null) {
      inputDataset = (DataSet) mapReduceContext.getInputDataset();
    } else  {
      // trying to init input dataset from spec
      String inputDataSetName = mapReduceContext.getSpecification().getInputDataSet();
      if (inputDataSetName != null) {
        inputDataset = mapReduceContext.getDataSet(inputDataSetName);
        // We checked on validation phase that it implements BatchReadable
        mapReduceContext.setInput((BatchReadable) inputDataset, ((BatchReadable) inputDataset).getSplits());
      }
    }

    if (inputDataset != null) {
      LOG.debug("Using dataset {} as input for mapreduce job", inputDataset.getName());
      DataSetInputFormat.setInput(jobConf, inputDataset);
    }
    return inputDataset;
  }

  private static final class MapReduceProgramController extends AbstractProgramController {
    MapReduceProgramController(BasicMapReduceContext context) {
      super(context.getProgramName(), context.getRunId());
    }

    @Override
    protected void doSuspend() throws Exception {
      // No-op
    }

    @Override
    protected void doResume() throws Exception {
      // No-op
    }

    @Override
    protected void doStop() throws Exception {
      // When job is stopped by controller doStop() method, the stopping() method of listener is also called.
      // That is where we kill the job, so no need to do any extra job in doStop().
    }

    @Override
    protected void doCommand(String name, Object value) throws Exception {
      // No-op
    }
  }

  private static Location buildJobJar(MapReduce job, Location jobJarLocation) throws IOException {
    ApplicationBundler appBundler = new ApplicationBundler(Lists.newArrayList("org.apache.hadoop"));
    Location appFabricDependenciesJarLocation = jobJarLocation.getTempFile(".job.jar");

    LOG.debug("Creating job jar: " + appFabricDependenciesJarLocation.toURI());
    appBundler.createBundle(appFabricDependenciesJarLocation,
                            job.configure().getClass(),
                            DataSetOutputFormat.class, DataSetInputFormat.class,
                            MapperWrapper.class, ReducerWrapper.class);
    merge(appFabricDependenciesJarLocation, jobJarLocation);
    return appFabricDependenciesJarLocation;
  }

  private static void merge(Location otherJar, Location programJar) throws IOException {
    // The resulting jar is a merge of both jars, but it uses programJar's manifest, which is essential to build
    // Program instance from the jar.

    // Write the jar to local tmp file first
    File tmpJar = File.createTempFile(otherJar.getName(), ".tmp");
    try {
      // this looks ugly, but we have to create source stream first to get manifest
      JarInputStream programJarIs = new JarInputStream(programJar.getInputStream());
      try {
        JarOutputStream jarOut = new JarOutputStream(new FileOutputStream(tmpJar), programJarIs.getManifest());
        try {
          // keeping track of added entries to prevent adding duplicates
          Set<String> addedEntries = Sets.newHashSet();

          // writing programJar contents first
          put(programJarIs, jarOut, addedEntries);

          // copying other jar
          addJarContents(otherJar, jarOut, addedEntries);

        } finally {
          jarOut.close();
        }
      } finally {
        programJarIs.close();
      }

      // Copy the tmp jar into destination.
      OutputStream os = new BufferedOutputStream(otherJar.getOutputStream());
      try {
        Files.copy(tmpJar, os);
      } finally {
        os.close();
      }
    } finally {
      if (!tmpJar.delete()) {
        LOG.warn("Failed to delete temp file: " + tmpJar);
        // failure should not affect other stuff
      }
    }
  }

  private static void addJarContents(Location src, JarOutputStream dest, Set<String> addedEntries) throws IOException {
    JarInputStream otherJarIs = new JarInputStream(src.getInputStream());
    try {
      put(otherJarIs, dest, addedEntries);
    } finally {
      otherJarIs.close();
    }
  }

  private static void put(JarInputStream src, JarOutputStream dest, Set<String> addedEntries) throws IOException {
    while (true) {
      ZipEntry nextEntry = src.getNextEntry();
      if (nextEntry == null) {
        break;
      }
      String entryName = nextEntry.getName();
      if (addedEntries.contains(entryName)) {
        continue;
      }
      addedEntries.add(entryName);
      // write entry
      dest.putNextEntry(new ZipEntry(entryName));
      ByteStreams.copy(src, dest);
      dest.closeEntry();
    }
  }

}
