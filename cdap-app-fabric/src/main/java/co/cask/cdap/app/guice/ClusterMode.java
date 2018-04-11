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
 */

package co.cask.cdap.app.guice;

/**
 * Defines the mode that the program container is getting executed.
 */
public enum ClusterMode {
  /**
   * The mode that program containers run in the same cluster as the CDAP,
   * hence can communicate directly with CDAP
   */
  ON_PREMISE,

  /**
   * The mode that program containers run in isolated cluster such that it cannot communicate with CDAP directly.
   */
  ISOLATED
}
