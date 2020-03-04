/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.launcher;

import java.util.Collection;
import java.util.Map;

/**
 * Job launcher to prepare and launch the job.
 */
public interface Launcher {

  /**
   * Initializes the launcher.
   *
   * @param context launcher context
   */
  void initialize(LauncherContext context);

  /**
   * Uses provided files to launch the job.
   *
   * @param launcherFiles files to be used to launch the job
   * @return unique job id
   */
  JobId launch(Collection<? extends LauncherFile> launcherFiles);

  /**
   * Gets job details.
   *
   * @param jobId id for the job
   * @return job details
   * @throws Exception thrown if any exception while getting job details
   */
  JobDetails getDetails(JobId jobId) throws Exception;

  /**
   * Provides all the jobs that are in running state.
   *
   * @return a map of job to job details
   * @throws Exception thrown if any exception while getting list of running jobs
   */
  Map<JobId, JobDetails> list() throws Exception;

  /**
   * Gracefully stops a running job.
   *
   * @param jobId job to be stopped
   * @throws Exception thrown if any exception while stopping the job
   */
  void stop(JobId jobId) throws Exception;

  /**
   * Forcefully kills a running job.
   *
   * @param jobId job to be killed
   * @throws Exception thrown if any exception while killing the job
   */
  void kill(JobId jobId) throws Exception;
}
