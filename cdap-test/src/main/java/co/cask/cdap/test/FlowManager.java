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

package co.cask.cdap.test;

/**
 * Instance for this class is for managing a running {@link co.cask.cdap.api.flow.Flow Flow}.
 */
public interface FlowManager {

  /**
   * Changes the number of flowlet instances.
   *
   * @param flowletName Name of the flowlet.
   * @param instances Number of instances to change to.
   */
  void setFlowletInstances(String flowletName, int instances);

  /**
   * Stops the running flow.
   */
  void stop();

  /**
   * Checks if Flow is Running
   */
  boolean isRunning();
}
