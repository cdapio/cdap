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
import com.continuuity.data.operation.executor.SynchronousTransactionAgent;
import com.continuuity.data.operation.executor.TransactionProxy;
import com.continuuity.filesystem.Location;
import com.continuuity.internal.app.runtime.AbstractListener;
import com.continuuity.internal.app.runtime.AbstractProgramController;
import com.continuuity.internal.app.runtime.DataSets;
import com.continuuity.internal.app.runtime.batch.dataset.DataSetInputFormat;
import com.continuuity.internal.app.runtime.batch.dataset.DataSetOutputFormat;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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
  private TransactionProxy transactionProxy;

  @Inject
  public MapReduceProgramRunner(CConfiguration cConf, Configuration hConf,
                                OperationExecutor opex,
                                LogWriter logWriter) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.opex = opex;
    CAppender.logWriter = logWriter;
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

    // TODO: start long running tx here (requires missing long-running txs)
    transactionProxy = new TransactionProxy();
    transactionProxy.setTransactionAgent(new SynchronousTransactionAgent(opex, opexContext));

    DataFabric dataFabric = new DataFabricImpl(opex, opexContext);
    DataSetInstantiator dataSetContext =
      new DataSetInstantiator(dataFabric, transactionProxy, program.getClassLoader());
    dataSetContext.setDataSets(Lists.newArrayList(program.getSpecification().getDataSets().values()));

    try {
      RunId runId = RunId.generate();
      final BasicMapReduceContext context =
        new BasicMapReduceContext(program, runId, DataSets.createDataSets(dataSetContext, spec.getDataSets()), spec);

      MapReduce job = (MapReduce) program.getMainClass().newInstance();
      context.injectFields(job);

      // note: this sets logging context on the thread level
      LoggingContextAccessor.setLoggingContext(context.getLoggingContext());

      controller = new MapReduceProgramController(context);

      LOG.info("Starting MapReduce job: " + context.toString());
      submit(job, program.getProgramJarLocation(), context);

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
      LOG.error("Failed to run mapreduce job", e);
      throw Throwables.propagate(e);
    }
  }

  private void submit(final MapReduce job, Location jobJarLocation, final BasicMapReduceContext context)
    throws Exception {
    jobConf = Job.getInstance(hConf);
    context.setJob(jobConf);
    // additional mapreduce job initialization at run-time
    job.beforeSubmit(context);

    // replace user's Mapper & Reducer's with our wrappers in job config
    wrapMapperClassIfNeeded(jobConf);
    wrapReducerClassIfNeeded(jobConf);

    // set input/output datasets info
    DataSet inputDataset = setInputDataSetIfNeeded(jobConf, context);
    DataSet outputDataset = setOutputDataSetIfNeeded(jobConf, context);

    boolean useDataSetAsInputOrOutput = inputDataset != null || outputDataset != null;
    if (useDataSetAsInputOrOutput) {
      MapReduceContextProvider contextProvider = new MapReduceContextProvider(jobConf);
      contextProvider.set(context, cConf);
    }

    // TODO: consider using approach that Weave uses: package all jars with submitted job all the time
    // adding continuuity jars to classpath (which are located/cached on hdfs to avoid redundant copying with every job)
    addContinuuityJarsToClasspath(jobConf);

    jobConf.setJar(jobJarLocation.toURI().getPath());

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
        }
      }
    }.start();
  }

  private void addContinuuityJarsToClasspath(Job jobConf) throws IOException {
    // ideally single line commented out below should be enough, but
    // due to yarn bug (MAPREDUCE-4740) we need to do it the ugly way
    // jobConf.addArchiveToClassPath(new Path("/continuuity/lib.zip"));

    FileSystem fs = FileSystem.get(hConf);

    Path libDir = new Path("/continuuity/lib");
    if (!fs.exists(libDir)) {
      LOG.warn("/continuuity/lib does NOT exist, only job jar is going to be in classpath of mapreduce tasks");
      return;
    }
    RemoteIterator<LocatedFileStatus> it = fs.listFiles(libDir, false);
    while (it.hasNext()) {
      LocatedFileStatus file = it.next();
      jobConf.addFileToClassPath(file.getPath());
    }
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
        transactionProxy.getTransactionAgent().finish();
      } else {
        transactionProxy.getTransactionAgent().abort();
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
}
