/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.api.spark.service;

import co.cask.cdap.api.spark.SparkExecutionContext;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * A base context interface for providing access to {@link SparkContext} and CDAP functionality that are
 * common to Spark 1 and Spark 2.
 */
public interface SparkHttpServiceContextBase extends SparkExecutionContext {

  /**
   * Returns the {@link SparkContext} object created in the Spark driver.
   */
  SparkContext getSparkContext();

  /**
   * Returns the {@link JavaSparkContext} wrapper for the {@link SparkContext} object created in the Spark driver.
   */
  JavaSparkContext getJavaSparkContext();

  /**
   * Returns a {@link SparkHttpServicePluginContext} for adding and using plugins.
   */
  @Override
  SparkHttpServicePluginContext getPluginContext();
}
