package com.continuuity.internal.app.runtime.batch;

import com.continuuity.api.batch.MapReduce;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.common.Cancellable;

/**
 * Performs the actual execution of mapreduce job.
 */
public interface MapReduceRuntimeService {
  /**
   * Submits the mapreduce job for execution.
   * @param job job to run
   * @param jobJarLocation location of the job jar
   * @param context runtime context
   * @throws Exception
   */
  Cancellable submit(MapReduce job, Location jobJarLocation, BasicMapReduceContext context, JobFinishCallback callback)
    throws Exception;

  public static interface JobFinishCallback {
    void onFinished(boolean success);
  }
}
