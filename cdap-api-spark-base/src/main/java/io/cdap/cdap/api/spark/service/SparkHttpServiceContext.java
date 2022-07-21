/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.cdap.api.spark.service;

import org.apache.spark.sql.SparkSession;

/**
 * Context interface for providing access to {@link SparkSession} and CDAP functionality.
 */
public interface SparkHttpServiceContext extends SparkHttpServiceContextBase {

  /**
   * Returns a {@link SparkSession} for operating with DataFrames.
   */
  SparkSession getSparkSession();
}
