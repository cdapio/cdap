/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.api.spark;

/**
 * Creates a {@link SparkContext} depending upon the user's job type (Java or Scala). This {@link SparkContext} is
 * then passed to the user.
 */
public interface SparkContextFactory {
  /**
   * Returns an appropriate CDAP custom {@link SparkContext} depending upon the type of Apache Spark
   * Context passed to it
   *
   * @param context the Apache Spark context which can be either SparkContext (for scala job) or JavaSparkContext
   *                (for Java jobs)
   * @param <T> type of context
   * @return custom {@link SparkContext}
   */
  <T> SparkContext create(T context);
}
