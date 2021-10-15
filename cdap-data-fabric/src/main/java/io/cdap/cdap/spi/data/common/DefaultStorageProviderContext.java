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

package io.cdap.cdap.spi.data.common;

import io.cdap.cdap.api.metrics.MetricsCollector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.spi.data.StorageProviderContext;

import java.util.Collections;
import java.util.Map;

/**
 * Default implementation of the {@link StorageProviderContext}.
 */
final class DefaultStorageProviderContext implements StorageProviderContext {

  private final Map<String, String> cConf;
  private final Map<String, String> sConf;
  private final MetricsCollector metricsCollector;

  DefaultStorageProviderContext(CConfiguration cConf, SConfiguration sConf,
                                String storageImpl, MetricsCollector metricsCollector) {
    String propertiesPrefix = Constants.Dataset.STORAGE_EXTENSION_PROPERTY_PREFIX + storageImpl + ".";
    this.cConf = Collections.unmodifiableMap(cConf.getPropsWithPrefix(propertiesPrefix));
    this.sConf = Collections.unmodifiableMap(sConf.getPropsWithPrefix(propertiesPrefix));
    this.metricsCollector = metricsCollector;
  }

  @Override
  public MetricsCollector getMetricsCollector() {
    return metricsCollector;
  }

  @Override
  public Map<String, String> getConfiguration() {
    return cConf;
  }

  @Override
  public Map<String, String> getSecurityConfiguration() {
    return sConf;
  }
}
