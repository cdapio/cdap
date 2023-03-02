/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.api.metrics;

import io.cdap.cdap.api.PlatformInfo;
import java.util.Map;

/**
 * Context passed to {@link MetricsWriter#initialize(MetricsWriterContext)}.
 */
public interface MetricsWriterContext {

  //TODO -  Remove this after the latest jar is published
  /**
   * CDAP version label
   */
  String CDAP_VERSION = "cdap.version";

  /**
   * Properties are derived from the CDAP configuration. Configuration file path will be added as an
   * entry in the  properties.
   *
   * @return unmodifiable properties for the metrics writer.
   */
  Map<String, String> getProperties();

  /**
   * @return encapsulated {@link MetricsContext}.
   */
  MetricsContext getMetricsContext();

  /**
   * Provide version string of the CDAP platform to be included in metrics if required
   *
   * @return String value of the platform version
   */
  default String getPlatformVersion() {
    return PlatformInfo.getVersion().toString();
  }
}
