package com.continuuity.internal.app.runtime.batch;

import com.continuuity.api.mapreduce.MapReduce;
import org.apache.twill.common.Cancellable;
import org.apache.twill.filesystem.Location;

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

  /**
   * Interface for receiving callback when map reduce job finished.
   */
  public static interface JobFinishCallback {
    void onFinished(boolean success);
  }
}
