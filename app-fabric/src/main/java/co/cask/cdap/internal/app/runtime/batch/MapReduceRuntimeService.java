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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.mapreduce.MapReduce;
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
