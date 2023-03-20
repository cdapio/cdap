/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.spi.metadata;

import io.cdap.cdap.api.annotation.Beta;

/**
 * Defines a way to collect provisioner metrics.
 */
@Beta
public interface MetadataConsumerMetrics {
  
  /**
   * Increases the value of the specific metric by delta.
   * 
   * @param metricName Name of the counter. Use alphanumeric characters in metric names.
   * @param delta The value to increase by.
   */
  void increment(String metricName, long delta);
}
