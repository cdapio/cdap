/*
 * Copyright © 2014 Cask Data, Inc.
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

import org.apache.spark.scheduler.JobResult;
import org.apache.spark.scheduler.JobSucceeded;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerBlockManagerAdded;
import org.apache.spark.scheduler.SparkListenerBlockManagerRemoved;
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate;
import org.apache.spark.scheduler.SparkListenerExecutorMetricsUpdate;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.SparkListenerTaskGettingResult;
import org.apache.spark.scheduler.SparkListenerTaskStart;
import org.apache.spark.scheduler.SparkListenerUnpersistRDD;

/**
 * Listens to events on running Spark Programs. Currently this listener only perform operations on {@link
 * SparkListener#onJobEnd(SparkListenerJobEnd)}
 */
class SparkProgramListener implements SparkListener {
  /**
   * Called when a stage completes successfully or fails, with information on the completed stage.
   *
   * @param stageCompleted
   */
  @Override
  public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
    //no-op
  }

  /**
   * Called when a stage is submitted
   *
   * @param stageSubmitted
   */
  @Override
  public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
    //no-op
  }

  /**
   * Called when a task starts
   *
   * @param taskStart
   */
  @Override
  public void onTaskStart(SparkListenerTaskStart taskStart) {
    //no-op
  }

  /**
   * Called when a task begins remotely fetching its result (will not be called for tasks that do
   * not need to fetch the result remotely).
   *
   * @param taskGettingResult
   */
  @Override
  public void onTaskGettingResult(SparkListenerTaskGettingResult taskGettingResult) {
    //no-op
  }

  /**
   * Called when a task ends
   *
   * @param taskEnd
   */
  @Override
  public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
    //no-op
  }

  /**
   * Called when a job starts
   *
   * @param jobStart
   */
  @Override
  public void onJobStart(SparkListenerJobStart jobStart) {
    //no-op
  }

  /**
   * Called when a job ends
   * Operations which are sent to worker node in Spark is called a job. So a Spark Program consists of multiple jobs.
   * For a spark program to succeed all the jobs have to succeed. If a job fails spark retries for a number of times
   * and if the job still fails the whole spark program fails.
   *
   * @param jobEnd {@link JobResult} of this job
   */
  @Override
  public void onJobEnd(SparkListenerJobEnd jobEnd) {
    if (JobSucceeded.canEqual(jobEnd.jobResult())) {
      SparkProgramWrapper.setSparkProgramSuccessful(true);
    } else {
      SparkProgramWrapper.setSparkProgramSuccessful(false);
    }
  }

  /**
   * Called when environment properties have been updated
   *
   * @param environmentUpdate
   */
  @Override
  public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate environmentUpdate) {
    //no-op
  }

  /**
   * Called when a new block manager has joined
   *
   * @param blockManagerAdded
   */
  @Override
  public void onBlockManagerAdded(SparkListenerBlockManagerAdded blockManagerAdded) {
    //no-op
  }

  /**
   * Called when an existing block manager has been removed
   *
   * @param blockManagerRemoved
   */
  @Override
  public void onBlockManagerRemoved(SparkListenerBlockManagerRemoved blockManagerRemoved) {
    //no-op
  }

  /**
   * Called when an RDD is manually unpersisted by the application
   *
   * @param unpersistRDD
   */
  @Override
  public void onUnpersistRDD(SparkListenerUnpersistRDD unpersistRDD) {
    //no-op
  }

  /**
   * Called when the application starts
   *
   * @param applicationStart
   */
  @Override
  public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
    //no-op
  }

  /**
   * Called when the application ends
   *
   * @param applicationEnd
   */
  @Override
  public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
    //no-op
  }

  /**
   * Called when the driver receives task metrics from an executor in a heartbeat.
   *
   * @param executorMetricsUpdate
   */
  @Override
  public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate executorMetricsUpdate) {
    //no-op
  }
}
