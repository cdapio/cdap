package com.continuuity.internal.app.runtime.batch.inmemory;

import com.continuuity.api.batch.MapReduce;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.internal.app.runtime.batch.BasicMapReduceContext;
import com.continuuity.internal.app.runtime.batch.MapReduceRuntimeService;
import com.continuuity.internal.app.runtime.batch.dataset.DataSetInputFormat;
import com.continuuity.internal.app.runtime.batch.dataset.DataSetOutputFormat;
import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.filesystem.Location;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs mapreduce job using {@link org.apache.hadoop.mapred.LocalJobRunner}
 */
public class InMemoryMapReduceRuntimeService implements MapReduceRuntimeService {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryMapReduceRuntimeService.class);

  private final org.apache.hadoop.conf.Configuration conf;

  @Inject
  public InMemoryMapReduceRuntimeService() {
    conf = new org.apache.hadoop.conf.Configuration();
    conf.addResource("mapred-site-local.xml");
    conf.reloadConfiguration();
  }

  @Override
  public Cancellable submit(final MapReduce job, Location jobJarLocation, final BasicMapReduceContext context,
                            final JobFinishCallback callback)
    throws Exception {
    final Job jobConf = Job.getInstance(conf);
    context.setJob(jobConf);
    // additional mapreduce job initialization at run-time
    job.beforeSubmit(context);

    DataSet inputDataset = setInputDataSetIfNeeded(jobConf, context);
    DataSet outputDataset = setOutputDataSetIfNeeded(jobConf, context);

    boolean useDataSetAsInputOrOutput = inputDataset != null || outputDataset != null;
    if (useDataSetAsInputOrOutput) {
      MapReduceContextAccessor.setRunId(jobConf.getConfiguration(), context.getRunId().getId());
      MapReduceContextAccessor.put(context.getRunId().getId(), context);
    }

    // adding job jar to classpath
    jobConf.addArchiveToClassPath(new Path(jobJarLocation.toURI().getPath()));

    new Thread() {
      @Override
      public void run() {
        try {
          LoggingContextAccessor.setLoggingContext(context.getLoggingContext());
          boolean success;
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
          callback.onFinished(true);
        }
      }
    }.start();

    return new Cancellable() {
      @Override
      public void cancel() {
        try {
          jobConf.killJob();
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
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

  private DataSet setInputDataSetIfNeeded(Job jobConf, BasicMapReduceContext mapReduceContext) {
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
}
