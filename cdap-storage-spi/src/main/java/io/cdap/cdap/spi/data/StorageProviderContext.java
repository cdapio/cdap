/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.spi.data;

import io.cdap.cdap.api.metrics.MetricsCollector;

import java.util.Map;

/**
 * The context for {@link StorageProvider} to interact with the CDAP platform.
 */
public interface StorageProviderContext {

  /**
   * A {@link MetricsCollector} for the storage provider to emit metrics.
   */
  MetricsCollector getMetricsCollector();

  /**
   * Configurations for the storage provider. It contains all the CDAP configurations that are prefixed with
   * {@code data.storage.properties.[storage_provider_name].} with the prefixed stripped.
   */
  Map<String, String> getConfiguration();

  /**
   * Configurations for the storage provider. It contains all the CDAP security configurations that are prefixed with
   * {@code data.storage.properties.[storage_provider_name].} with the prefixed stripped.
   */
  Map<String, String> getSecurityConfiguration();
}
