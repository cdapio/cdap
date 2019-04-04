/*
 * Copyright © 2018 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.internal.bootstrap.executor;

import com.google.gson.JsonObject;
import io.cdap.cdap.proto.bootstrap.BootstrapStepResult;

/**
 * Executes a bootstrap step.
 */
public interface BootstrapStepExecutor {

  /**
   * Execute the bootstrap step.
   *
   * @param label the step label
   * @param argumentObject the object representing the arguments for the step
   * @return the result of execution
   * @throws InterruptedException if the step was interrupted
   */
  BootstrapStepResult execute(String label, JsonObject argumentObject) throws InterruptedException;
}
