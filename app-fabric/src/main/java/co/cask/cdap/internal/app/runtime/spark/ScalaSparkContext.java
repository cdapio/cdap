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

import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.RecordScannable;
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
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.NewHadoopRDD;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.List;

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
   * Gets a {@link Dataset} as a {@link NewHadoopRDD}
   *
   * @param datasetName the name of the {@link Dataset} to be read as an RDD
   * @param kClass      the key class
   * @param vClass      the value class
   * @param <T>         type of the RDD
   * @return the {@link NewHadoopRDD} created from the dataset to be read
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
      RecordScannable curDataset = (RecordScannable) basicSparkContext.getDataSet(datasetName);
      inputSplits = curDataset.getSplits();
    }
    hConf.setClass(MRJobConfig.INPUT_FORMAT_CLASS_ATTR, DataSetInputFormat.class, InputFormat.class);
    hConf.set(DataSetInputFormat.HCONF_ATTR_INPUT_DATASET, datasetName);
    hConf.set(SparkContextConfig.HCONF_ATTR_INPUT_SPLIT_CLASS, inputSplits.get(0).getClass().getName());
    hConf.set(SparkContextConfig.HCONF_ATTR_INPUT_SPLITS, new Gson().toJson(inputSplits));

    return (T) apacheContext.newAPIHadoopFile(datasetName, DataSetInputFormat.class, kClass, vClass, hConf);
  }

  /**
   * Stores a {@link RDD} to {@link Dataset}
   *
   * @param rdd         the {@link RDD} to be stored
   * @param datasetName the name of the {@link Dataset} where the RDD should be stored
   * @param kClass      the key class
   * @param vClass      the value class
   * @param <T>         type of RDD
   */
  @Override
  public <T> void writeToDataset(T rdd, String datasetName, Class<?> kClass, Class<?> vClass) {
    writeToDatasetHelper(rdd, datasetName, kClass, vClass);
  }

  private <T, K, V> void writeToDatasetHelper(T rdd, String datasetName, Class<K> kClass, Class<V> vClass) {
    ClassTag<K> kClassTag = ClassTag$.MODULE$.apply(kClass);
    ClassTag<V> vClassTag = ClassTag$.MODULE$.apply(vClass);
    PairRDDFunctions pairRDD = new PairRDDFunctions<K, V>((RDD<Tuple2<K, V>>) rdd, kClassTag, vClassTag, null);
    Configuration hConf = new Configuration(getHConf());
    hConf.set(DataSetOutputFormat.HCONF_ATTR_OUTPUT_DATASET, datasetName);
    hConf.setClass(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, DataSetOutputFormat.class, OutputFormat.class);
    pairRDD.saveAsNewAPIHadoopFile(datasetName, kClass, vClass, DataSetOutputFormat.class, hConf);
  }

  /**
   * Return a {@link SparkContext} object
   *
   * @param <T> type of Apache Spark Context which is {@link SparkContext} here
   * @return the {@link SparkContext}
   */
  @Override
  public <T> T getBaseSparkContext() {
    return (T) apacheContext;
  }
}
