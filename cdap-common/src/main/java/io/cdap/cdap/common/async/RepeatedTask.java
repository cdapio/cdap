/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.common.async;

/**
 * Interface to represent a task that get executed repeatedly.
 */
public interface RepeatedTask {

  /**
   * Executes the task once.
   *
   * @return number of milliseconds to pass before calling this method again for the next execution.
   *         Returning a value that is smaller than {@code 0} to indicate the task executing is completed.
   * @throws Exception if the task execution failed.
   */
  long executeOnce() throws Exception;
}
