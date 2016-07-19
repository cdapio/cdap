/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.api.action;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.plugin.PluginContext;

/**
 * Represents the context available to the action plugin during runtime.
 */
public interface ActionContext extends Transactional, PluginContext {

  /**
   * Returns the logical start time of the batch job which triggers this instance of an action.
   * Logical start time is the time when the triggering Batch job is supposed to start if it is
   * started by the scheduler. Otherwise it would be the current time when the action runs.
   *
   * @return Time in milliseconds since epoch time (00:00:00 January 1, 1970 UTC).
   */
  long getLogicalStartTime();

  /**
   * Return the arguments which can be updated.
   */
  SettableArguments getArguments();
}
