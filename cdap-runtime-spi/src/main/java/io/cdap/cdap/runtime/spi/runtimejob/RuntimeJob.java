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
 * Represents runtime job that will be executed.
 */
public interface RuntimeJob {
  /**
   * This method will be responsible for running the runtime job using underlying runner.
   *
   * @param runtimeJobContext runtime job context
   * @throws Exception thrown if any error while job execution
   */
  void run(RuntimeJobContext runtimeJobContext) throws Exception;
}
