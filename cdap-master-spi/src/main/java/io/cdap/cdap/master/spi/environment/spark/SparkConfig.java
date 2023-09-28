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
import javax.annotation.Nullable;

/**
 * Represents environment specific spark submit configurations.
 */
public class SparkConfig {

  public static final String DRIVER_ENV_PREFIX = "spark.kubernetes.driverEnv.";
  private final String master;
  private final URI sparkJobFile;
  private final Map<String, String> configs;
  private final SparkDriverWatcher sparkDriverWatcher;
  private final String extraJavaOpts;

  /**
   * Create spark configurations for a specific environment
   *
   * @param master the spark master
   * @param sparkJobFile location of the job file
   * @param configs SparkConf properties that should be added to the spark job
   * @param sparkDriverWatcher watcher for the Spark driver
   * @param extraJavaOpts extra java options that should be added to the driver and executor
   */
  public SparkConfig(String master, URI sparkJobFile, Map<String, String> configs,
      SparkDriverWatcher sparkDriverWatcher, @Nullable String extraJavaOpts) {
    this.master = master;
    this.sparkJobFile = sparkJobFile;
    this.configs = Collections.unmodifiableMap(new HashMap<>(configs));
    this.sparkDriverWatcher = sparkDriverWatcher;
    this.extraJavaOpts = extraJavaOpts;
  }

  /**
   * Returns spark master base path. This should be used to set spark master url:
   * https://spark.apache.org/docs/latest/submitting-applications.html#master-urls
   */
  public String getMaster() {
    return master;
  }

  /**
   * Returns URI for spark job file. If spark job file doesnt need to come from master environment,
   * this method should return null.
   */
  @Nullable
  public URI getSparkJobFile() {
    return sparkJobFile;
  }

  /**
   * Returns additional environment specific spark submit configurations. These will be added to
   * --conf of spark submit. These will *override* any value specified by the application. Note
   * that extra java options should be specified separately rather than returned
   * as a spark configuration here.
   */
  public Map<String, String> getConfigs() {
    return configs;
  }

  /**
   * Returns driver watcher thread.
   */
  public SparkDriverWatcher getSparkDriverWatcher() {
    return sparkDriverWatcher;
  }

  /**
   * Returns extra java options to set for the driver and executor. This will be prepended to any
   * extra java options set for the application.
   */
  @Nullable
  public String getExtraJavaOpts() {
    return extraJavaOpts;
  }
}
