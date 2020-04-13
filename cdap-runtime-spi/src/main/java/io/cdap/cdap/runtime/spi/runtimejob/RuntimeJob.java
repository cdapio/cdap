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

/**
 * Represents a job that will be executed on a given runtime environment.
 */
public interface RuntimeJob {

  /**
   * This method will be responsible for submitting the runtime job to underlying runner.
   *
   * @param runtimeJobEnvironment runtime job environment
   * @throws Exception thrown if any error while job execution
   */
  void run(RuntimeJobEnvironment runtimeJobEnvironment) throws Exception;

  /**
   * This method will be called when there is an explicit stop to terminate the running program.
   * This method should block until the runtime job is stopped.
   */
  void requestStop();
}
