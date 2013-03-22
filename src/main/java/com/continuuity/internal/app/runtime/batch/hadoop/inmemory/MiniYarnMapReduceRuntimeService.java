package com.continuuity.internal.app.runtime.batch.hadoop.inmemory;

import com.continuuity.api.batch.hadoop.HadoopMapReduceJobSpecification;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.batch.hadoop.HadoopMapReduceJob;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.base.Cancellable;
import com.continuuity.filesystem.Location;
import com.continuuity.internal.app.runtime.batch.BasicBatchContext;
import com.continuuity.internal.app.runtime.batch.hadoop.MapReduceRuntimeService;
import com.continuuity.internal.app.runtime.batch.hadoop.BasicHadoopMapReduceJobContext;
import com.continuuity.internal.app.runtime.batch.hadoop.dataset.DataSetInputFormat;
import com.continuuity.internal.app.runtime.batch.hadoop.dataset.DataSetInputOutputFormatHelper;
import com.continuuity.internal.app.runtime.batch.hadoop.dataset.DataSetOutputFormat;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.mapreduce.v2.TestMRJobs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs mapreduce job using {@link MiniMRYarnCluster}
 */
public class MiniYarnMapReduceRuntimeService implements MapReduceRuntimeService {
  private static final Logger LOG = LoggerFactory.getLogger(MiniYarnMapReduceRuntimeService.class);

  private final MiniMRYarnCluster mrCluster;
  private final org.apache.hadoop.conf.Configuration conf;
  private boolean started = false;

  @Inject
  public MiniYarnMapReduceRuntimeService() {
    mrCluster = new MiniMRYarnCluster(TestMRJobs.class.getName(), 1);
    conf = new org.apache.hadoop.conf.Configuration();
  }

  @Override
  public void startUp() {
    mrCluster.init(conf);
    mrCluster.start();
    started = true;
  }

  @Override
  public void shutDown() {
    mrCluster.stop();
    started = false;
  }

  @Override
  public Cancellable submit(final HadoopMapReduceJob job, HadoopMapReduceJobSpecification spec,
                     Location jobJarLocation, BasicBatchContext context, final JobFinishCallback callback)
    throws Exception {
    // todo
//    if (!started) {
//      throw new IllegalStateException("MiniYarnMapReduceRuntimeService is not running. Start it first by invoking" +
//                                        " startUp() before submitting jobs.");
//    }

    final Job jobConf = Job.getInstance(conf);
    final BasicHadoopMapReduceJobContext hadoopMapReduceJobContext =
      new BasicHadoopMapReduceJobContext(spec, context, jobConf, context.getRunId());

    // additional mapreduce job initialization at run-time
    job.beforeSubmit(hadoopMapReduceJobContext);

    DataSet inputDataset = setInputDataSetIfNeeded(jobConf, hadoopMapReduceJobContext);

    DataSet outputDataset = setOutputDataSetIfNeeded(jobConf, hadoopMapReduceJobContext);

    boolean useDataSetAsInputOrOutput = inputDataset != null || outputDataset != null;
    if (useDataSetAsInputOrOutput) {
      DataSetInputOutputFormatHelper.writeRunId(jobConf.getConfiguration(), context.getRunId().getId());
      DataSetInputOutputFormatHelper.add(context.getRunId().getId(), hadoopMapReduceJobContext);
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

          job.onFinish(success, hadoopMapReduceJobContext);
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

  private DataSet setOutputDataSetIfNeeded(Job jobConf, BasicHadoopMapReduceJobContext hadoopMapReduceJobContext) {
    DataSet outputDataset = null;
    // whatever was set into hadoopMapReduceJobContext e.g. during beforeSubmit(..) takes precedence
    if (hadoopMapReduceJobContext.getOutputDataset() != null) {

    } else {
      // trying to init output dataset from spec
      String outputDataSetName = hadoopMapReduceJobContext.getSpecification().getOutputDataSet();
      if (outputDataSetName != null) {
        // We checked on validation phase that it implements BatchWritable
        outputDataset = hadoopMapReduceJobContext.getDataSet(outputDataSetName);
        hadoopMapReduceJobContext.setOutput((BatchWritable) outputDataset);
      }
    }

    if (outputDataset != null) {
      // Setting dataset input format for the jobConf. User can override it if needed.
      DataSetOutputFormat.setOutput(jobConf, outputDataset);
    }
    return outputDataset;
  }

  private DataSet setInputDataSetIfNeeded(Job jobConf, BasicHadoopMapReduceJobContext hadoopMapReduceJobContext) {
    DataSet inputDataset = null;
    // whatever was set into hadoopMapReduceJobContext e.g. during beforeSubmit(..) takes precedence
    if (hadoopMapReduceJobContext.getInputDataset() != null) {
      inputDataset = (DataSet) hadoopMapReduceJobContext.getInputDataset();
    } else  {
      // trying to init input dataset from spec
      String inputDataSetName = hadoopMapReduceJobContext.getSpecification().getInputDataSet();
      if (inputDataSetName != null) {
        inputDataset = hadoopMapReduceJobContext.getDataSet(inputDataSetName);
        // We checked on validation phase that it implements BatchReadable
        hadoopMapReduceJobContext.setInput((BatchReadable) inputDataset, ((BatchReadable) inputDataset).getSplits());
      }
    }

    if (inputDataset != null) {
      DataSetInputFormat.setInput(jobConf, inputDataset);
    }
    return inputDataset;
  }
}
