/*
 * Copyright 2014 Cask Data, Inc.
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

import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.internal.app.runtime.spark.dataset.DataSetInputFormat;
import co.cask.cdap.internal.app.runtime.spark.dataset.DataSetOutputFormat;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.List;

/**
 * A concrete implementation of {@link AbstractSparkContext} which is used if the user's spark job is written in Java.
 */
class JavaSparkContext extends AbstractSparkContext {

  private final org.apache.spark.api.java.JavaSparkContext apacheContext;

  public JavaSparkContext(org.apache.spark.api.java.JavaSparkContext apacheContext, long logicalStartTime,
                          SparkSpecification spec, Arguments runtimeArguments) {
    super(logicalStartTime, spec, runtimeArguments);
    this.apacheContext = apacheContext;
  }

  /**
   * Gets a {@link Dataset} as a {@link JavaPairRDD}
   *
   * @param datasetName the name of the {@link Dataset} to be read as an RDD
   * @param kClass      the key class
   * @param vClass      the value class
   * @param <T>         type of the RDD
   * @return the {@link JavaPairRDD} created from the dataset to be read
   * @throws {@link IllegalArgumentException} if the dataset to be read does not implements {@link BatchReadable}
   */
  @Override
  public <T> T readFromDataset(String datasetName, Class<?> kClass, Class<?> vClass) {
    Configuration hConf = new Configuration(getHConf());
    Dataset dataset = basicSparkContext.getDataSet(datasetName);
    List<Split> inputSplits;
    if (dataset instanceof BatchReadable) {
      BatchReadable curDataset = (BatchReadable) basicSparkContext.getDataSet(datasetName);
      inputSplits = curDataset.getSplits();
    } else {
      throw new IllegalArgumentException("Failed to read dataset " + datasetName + ". The dataset does not implements" +
                                           " BatchReadable");
    }
    hConf.setClass(MRJobConfig.INPUT_FORMAT_CLASS_ATTR, DataSetInputFormat.class, InputFormat.class);
    hConf.set(DataSetInputFormat.HCONF_ATTR_INPUT_DATASET, datasetName);
    hConf.set(SparkContextConfig.HCONF_ATTR_INPUT_SPLIT_CLASS, inputSplits.get(0).getClass().getName());
    hConf.set(SparkContextConfig.HCONF_ATTR_INPUT_SPLITS, new Gson().toJson(inputSplits));

    return (T) apacheContext.newAPIHadoopFile(datasetName, DataSetInputFormat.class, kClass, vClass, hConf);
  }

  /**
   * Stores a {@link JavaPairRDD} to {@link Dataset}
   *
   * @param rdd         the {@link JavaPairRDD} to be stored
   * @param datasetName the name of the {@link Dataset} where the RDD should be stored
   * @param kClass      the key class
   * @param vClass      the value class
   * @param <T>         type of RDD
   */
  @Override
  public <T> void writeToDataset(T rdd, String datasetName, Class<?> kClass, Class<?> vClass) {
    Configuration hConf = new Configuration(getHConf());
    hConf.set(DataSetOutputFormat.HCONF_ATTR_OUTPUT_DATASET, datasetName);
    hConf.setClass(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, DataSetOutputFormat.class, OutputFormat.class);
    ((JavaPairRDD) rdd).saveAsNewAPIHadoopFile(datasetName, kClass, vClass, DataSetOutputFormat.class, hConf);
  }

  /**
   * Returns a {@link org.apache.spark.api.java.JavaSparkContext} object
   *
   * @param <T> type of Apache Spark Context which is {@link org.apache.spark.api.java.JavaSparkContext} here
   * @return the {@link org.apache.spark.api.java.JavaSparkContext}
   */
  @Override
  public <T> T getBaseSparkContext() {
    return (T) apacheContext;
  }
}
