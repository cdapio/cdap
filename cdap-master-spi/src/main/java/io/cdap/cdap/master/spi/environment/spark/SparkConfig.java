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

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

/**
 * Represents environment specific spark submit configurations.
 */
public class SparkConfig {
  public static final String DRIVER_ENV_PREFIX = "spark.kubernetes.driverEnv.";
  private final String master;
  private final URI sparkJobFile;
  private final CompletableFuture<Boolean> submitFuture;
  private final Map<String, String> configs;

  public SparkConfig(String master, URI sparkJobFile, CompletableFuture<Boolean> submitFuture,
                     Map<String, String> configs) {
    this.master = master;
    this.sparkJobFile = sparkJobFile;
    this.submitFuture = submitFuture;
    this.configs = Collections.unmodifiableMap(new HashMap<>(configs));
  }

  /**
   * Returns spark master base path. This should be used to set spark master url:
   * https://spark.apache.org/docs/latest/submitting-applications.html#master-urls
   */
  public String getMaster() {
    return master;
  }

  /**
   * Returns URI for spark job file. If spark job file doesn't need to come from master environment, this method
   * should return null.
   */
  @Nullable
  public URI getSparkJobFile() {
    return sparkJobFile;
  }

  public CompletableFuture<Boolean> getSubmitFuture() {
    return submitFuture;
  }

  /**
   * Returns additional environment specific spark submit configurations. These will be added to --conf of spark submit.
   */
  public Map<String, String> getConfigs() {
    return configs;
  }
}
