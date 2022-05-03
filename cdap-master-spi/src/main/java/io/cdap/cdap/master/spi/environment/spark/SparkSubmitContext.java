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
package io.cdap.cdap.master.spi.environment.spark;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Spark submit context for master environment.
 */
public class SparkSubmitContext {
  private final Map<String, SparkLocalizeResource> localizeResources;
  private final int driverVirtualCores;
  private final int executorVirtualCores;

  public SparkSubmitContext(Map<String, SparkLocalizeResource> localizeResources,
                            int driverVirtualCores, int executorVirtualCores) {
    this.localizeResources = Collections.unmodifiableMap(new HashMap<>(localizeResources));
    this.driverVirtualCores = driverVirtualCores;
    this.executorVirtualCores = executorVirtualCores;
  }

  public Map<String, SparkLocalizeResource> getLocalizeResources() {
    return localizeResources;
  }

  public int getDriverVirtualCores() {
    return driverVirtualCores;
  }

  public int getExecutorVirtualCores() {
    return executorVirtualCores;
  }
}
