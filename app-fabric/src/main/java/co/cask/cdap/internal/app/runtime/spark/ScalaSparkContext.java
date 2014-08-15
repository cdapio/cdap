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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.internal.app.runtime.batch.dataset.DataSetInputFormat;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.NewHadoopRDD;
import org.apache.spark.rdd.RDD;

/**
 * A concrete implementation of {@link AbstractSparkContext} which is used if the user's spark job is written in Scala.
 */
class ScalaSparkContext extends AbstractSparkContext {

  private final SparkContext apacheContext;

  public ScalaSparkContext(SparkContext apacheContext, long logicalStartTime, SparkSpecification spec,
                           Arguments runtimeArguments) {
    super(logicalStartTime, spec, runtimeArguments);
    this.apacheContext = apacheContext;
  }

  /**
   * Function to get a {@link Dataset} as a {@link NewHadoopRDD}
   *
   * @param datasetName the name of the {@link Dataset} to be read as an RDD
   * @param kClass      the key class
   * @param vClass      the value class
   * @param <T>         type of the RDD
   * @return the {@link NewHadoopRDD} created from the dataset to be read
   */
  @Override
  public <T> T readFromDataset(String datasetName, Class<?> kClass, Class<?> vClass) {
    return (T) apacheContext.newAPIHadoopFile(datasetName, DataSetInputFormat.class, kClass, vClass, gethConf());
  }

  /**
   * Function to store a {@link RDD} to {@link Dataset}
   *
   * @param rdd         the {@link RDD} to be stored
   * @param datasetName the name of the {@link Dataset} where the RDD should be stored
   * @param kClass      the key class
   * @param vClass      the value class
   * @param <T>         type of RDD
   */
  @Override
  public <T> void writeToDataset(T rdd, String datasetName, Class<?> kClass, Class<?> vClass) {
    //TODO: Add here when figured out
  }

  /**
   * Getter method to get Apache Spark's {@link SparkContext} object
   *
   * @param <T> type of Apache Spark Context which is {@link SparkContext} here
   * @return the {@link SparkContext}
   */
  @Override
  public <T> T getApacheSparkContext() {
    return (T) apacheContext;
  }
}
