package com.continuuity.internal.app.runtime.batch.hadoop.inmemory;

import com.continuuity.api.batch.hadoop.MapReduce;
import com.continuuity.api.batch.hadoop.MapReduceSpecification;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.base.Cancellable;
import com.continuuity.filesystem.Location;
import com.continuuity.internal.app.runtime.batch.BasicBatchContext;
import com.continuuity.internal.app.runtime.batch.hadoop.BasicMapReduceContext;
import com.continuuity.internal.app.runtime.batch.hadoop.MapReduceRuntimeService;
import com.continuuity.internal.app.runtime.batch.hadoop.dataset.DataSetInputFormat;
import com.continuuity.internal.app.runtime.batch.hadoop.dataset.DataSetInputOutputFormatHelper;
import com.continuuity.internal.app.runtime.batch.hadoop.dataset.DataSetOutputFormat;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs mapreduce job using {@link MiniMRYarnCluster}
 */
public class MiniYarnMapReduceRuntimeService extends AbstractIdleService implements MapReduceRuntimeService {
  private static final Logger LOG = LoggerFactory.getLogger(MiniYarnMapReduceRuntimeService.class);

  private final org.apache.hadoop.conf.Configuration conf;

  @Inject
  public MiniYarnMapReduceRuntimeService() {
    conf = new org.apache.hadoop.conf.Configuration();
  }

  @Override
  public void startUp() {
    // DO NOTHING
  }

  @Override
  public void shutDown() {
    // DO NOTHING
  }

  @Override
  public Cancellable submit(final MapReduce job, MapReduceSpecification spec,
                     Location jobJarLocation, BasicBatchContext context, final JobFinishCallback callback)
    throws Exception {
    final Job jobConf = Job.getInstance(conf);
    final BasicMapReduceContext mapReduceContext =
      new BasicMapReduceContext(spec, context, jobConf, context.getRunId());

    // additional mapreduce job initialization at run-time
    job.beforeSubmit(mapReduceContext);

    DataSet inputDataset = setInputDataSetIfNeeded(jobConf, mapReduceContext);

    DataSet outputDataset = setOutputDataSetIfNeeded(jobConf, mapReduceContext);

    boolean useDataSetAsInputOrOutput = inputDataset != null || outputDataset != null;
    if (useDataSetAsInputOrOutput) {
      DataSetInputOutputFormatHelper.writeRunId(jobConf.getConfiguration(), context.getRunId().getId());
      DataSetInputOutputFormatHelper.add(context.getRunId().getId(), mapReduceContext);
    }

    // adding job jar to classpath
    jobConf.addArchiveToClassPath(new Path(jobJarLocation.toURI().getPath()));

    new Thread() {
      @Override
      public void run() {
        try {
          boolean success;
          try {
           success = jobConf.waitForCompletion(true);
          } catch (InterruptedException e) {
            // nothing we can do now: we simply stopped watching for job completion...
            throw Throwables.propagate(e);
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }

          job.onFinish(success, mapReduceContext);
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
      // Setting dataset input format for the jobConf. User can override it if needed.
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
      DataSetInputFormat.setInput(jobConf, inputDataset);
    }
    return inputDataset;
  }
}
