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

package co.cask.cdap.operations.yarn;

import javax.management.MXBean;

/**
 * {@link MXBean} for reporting YARN memory.
 */
public interface YarnResourcesMXBean {
  /**
   * Returns the total memory in YARN.
   */
  long getTotalMemory();

  /**
   * Returns the used memory in YARN.
   */
  long getUsedMemory();

  /**
   * Returns the free memory in YARN.
   */
  long getFreeMemory();

  /**
   * Returns the total virtual cores in YARN.
   */
  int getTotalVCores();

  /**
   * Returns the used virtual cores in YARN.
   */
  int getUsedVCores();

  /**
   * Returns the free virtual cores in YARN.
   */
  int getFreeVCores();
}
