package com.continuuity.internal.app.runtime.batch.hadoop;

import com.continuuity.api.batch.hadoop.HadoopMapReduceJob;
import com.continuuity.api.batch.hadoop.HadoopMapReduceJobSpecification;
import com.continuuity.base.Cancellable;
import com.continuuity.filesystem.Location;
import com.continuuity.internal.app.runtime.batch.BasicBatchContext;

/**
 * Performs the actual execution of mapreduce job.
 */
public interface MapReduceRuntimeService {
  /**
   * Submits the mapreduce job for execution.
   * @param job job to run
   * @param spec job spec
   * @param jobJarLocation location of the job jar
   * @param context runtime context
   * @throws Exception
   */
  Cancellable submit(HadoopMapReduceJob job, HadoopMapReduceJobSpecification spec,
              Location jobJarLocation, BasicBatchContext context, JobFinishCallback callback) throws Exception;

  /**
   * Starts this service
   */
  void startUp();

  /**
   * Stops this service
   */
  void shutDown();

  public static interface JobFinishCallback {
    void onFinished(boolean success);
  }
}
