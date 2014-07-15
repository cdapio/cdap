/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.api.dataset.metrics;

import com.continuuity.api.annotation.Beta;

/**
 * Defines interface to be implemented by {@link com.continuuity.api.dataset.Dataset} implementations to
 * expose data ops metrics.
 */
@Beta
public interface MeteredDataset {
  /**
   * Sets data ops metrics collector
   * @param metricsCollector metrics collector
   */
  void setMetricsCollector(MetricsCollector metricsCollector);

  /**
   * Collector for data ops metrics.
   */
  static interface MetricsCollector {

    /**
     * Records read operation stats
     * @param opsCount number of ops performed
     * @param dataSizeInBytes size of data read
     */
    void recordRead(int opsCount, int dataSizeInBytes);

    /**
     * Records write operation stats
     * @param opsCount number of ops performed
     * @param dataSizeInBytes size of data written
     */
    void recordWrite(int opsCount, int dataSizeInBytes);
  }
}
