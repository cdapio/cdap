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

package io.cdap.cdap.runtime.spi.runtimejob;

import io.cdap.cdap.runtime.spi.ProgramRunInfo;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;

/**
 * Runtime job manager to prepare and launch the job.
 */
public interface RuntimeJobManager extends Closeable {
  /**
   * Initialize the clients for job launch.
   *
   * @throws Exception thrown if any exception while initializing the manager
   */
  void initialize() throws Exception;

  /**
   * This method uses runtime job information to launch the job.
   *
   * @param runtimeJobInfo runtime job information
   */
  void launch(RuntimeJobInfo runtimeJobInfo) throws Exception;

  /**
   * Gets job details. If the job does not exist, it should return empty job details.
   *
   * @param programRunInfo program run info
   * @return runtime job details
   * @throws Exception thrown if any exception while getting job details
   */
  Optional<RuntimeJobDetail> getDetail(ProgramRunInfo programRunInfo) throws Exception;

  /**
   * Provides all the jobs that are in running state. If there are no running jobs, it should return empty list.
   *
   * @return a list job details
   * @throws Exception thrown if any exception while getting list of running jobs
   */
  List<RuntimeJobDetail> list() throws Exception;

  /**
   * Gracefully stops a running job. If the job is already in terminal status, then this method should be a no-op. If
   * the job does not exist, this method should be a no-op.
   *
   * @param programRunInfo program run info
   * @throws Exception thrown if any exception while stopping the job
   */
  void stop(ProgramRunInfo programRunInfo) throws Exception;

  /**
   * Forcefully kills a running job. If the job is already in terminal status, then this method should be a no-op. If
   * the job does not exist, this method should be a no-op.
   *
   * @param programRunInfo program run info
   * @throws Exception thrown if any exception while killing the job
   */
  void kill(ProgramRunInfo programRunInfo) throws Exception;

  /**
   * This method is responsible to perform clean up for runtime manager.
   */
  @Override
  void close();
}
