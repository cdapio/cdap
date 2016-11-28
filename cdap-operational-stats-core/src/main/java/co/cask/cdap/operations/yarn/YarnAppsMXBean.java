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
 * {@link MXBean} for reporting YARN application stats.
 */
public interface YarnAppsMXBean {
  /**
   * Returns the number of new apps in YARN.
   */
  int getNew();

  /**
   * Returns the total number of apps in YARN.
   */
  int getTotal();

  /**
   * Returns the number of submitted apps in YARN.
   */
  int getSubmitted();

  /**
   * Returns the number of accepted apps in YARN.
   */
  int getAccepted();

  /**
   * Returns the number of running apps in YARN.
   */
  int getRunning();

  /**
   * Returns the number of finished apps in YARN.
   */
  int getFinished();

  /**
   * Returns the number of failed apps in YARN.
   */
  int getFailed();

  /**
   * Returns the number of killed apps in YARN.
   */
  int getKilled();
}
