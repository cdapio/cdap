/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.api.dataset.Dataset;

/**
 * Spark job execution context. This context is shared between CDAP and User's Spark job.
 * This interface exposes two prominent methods:
 * <ol>
 * <li>{@link SparkContext#readFromDataset(String, Class, Class)}: Allows user to read a {@link Dataset} as an
 * RDD</li>
 * <li>{@link SparkContext#writeToDataset(Object, String, Class, Class)}: Allows user to write a RDD to a {@link
 * Dataset}</li>
 * </ol>
 * Classes implementing this interface should also have a Spark Context member of appropriate type on which these
 * method acts.
 */
public interface SparkContext extends RuntimeContext {
  /**
   * @return The specification used to configure this {@link Spark} job instance.
   */
  SparkSpecification getSpecification();

  /**
   * Returns the logical start time of this Spark job. Logical start time is the time when this Spark
   * job is supposed to start if this job is started by the scheduler. Otherwise it would be the current time when the
   * job runs.
   *
   * @return Time in milliseconds since epoch time (00:00:00 January 1, 1970 UTC).
   */
  long getLogicalStartTime();

  /**
   * Create a Spark RDD that uses {@link Dataset} as input source
   *
   * @param datasetName the name of the {@link Dataset} to be read as an RDD
   * @param kClass      the key class
   * @param vClass      the value class
   * @param <T>         type of RDD
   * @return the RDD created from Dataset
   * @throws UnsupportedOperationException if the SparkContext is not yet initialized
   */
  <T> T readFromDataset(String datasetName, Class<?> kClass, Class<?> vClass);

  /**
   * Writes a Spark RDD to {@link Dataset}
   *
   * @param rdd         the rdd to be stored
   * @param datasetName the name of the {@link Dataset} where the RDD should be stored
   * @param kClass      the key class
   * @param vClass      the value class
   * @param <T>         type of RDD
   * @throws UnsupportedOperationException if the SparkContext is not yet initialized
   */
  <T> void writeToDataset(T rdd, String datasetName, Class<?> kClass, Class<?> vClass);

  /**
   * Returns
   * <a href="http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.api.java.JavaSparkContext">
   * JavaSparkContext</a> or
   * <a href="http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.SparkContext">SparkContext</a>
   * depending on user's job type.
   *
   * @param <T> the type of Spark Context
   * @return the Spark Context
   */
  <T> T getOriginalSparkContext();

  /**
   * Returns value of the given argument key as a String[]
   *
   * @param argsKey {@link String} which is the key for the argument
   * @return String[] containing all the arguments which is indexed by their position as they were supplied
   */
  public String[] getRuntimeArguments(String argsKey);
}
