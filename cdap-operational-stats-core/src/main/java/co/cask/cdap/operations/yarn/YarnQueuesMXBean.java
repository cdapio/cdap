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
 * {@link MXBean} for reporting YARN queue stats.
 */
public interface YarnQueuesMXBean {
  /**
   * Returns the total queues in YARN.
   */
  int getTotal();

  /**
   * Returns the stopped queues in YARN.
   */
  int getStopped();

  /**
   * Returns the running queues in YARN.
   */
  int getRunning();
}
