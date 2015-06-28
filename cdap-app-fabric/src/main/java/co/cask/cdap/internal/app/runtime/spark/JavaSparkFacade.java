/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.internal.app.runtime.spark.dataset.SparkDatasetOutputFormat;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Map;

/**
 * Implements {@link SparkFacade} with a {@link JavaSparkContext}.
 */
final class JavaSparkFacade implements SparkFacade {

  private final org.apache.spark.api.java.JavaSparkContext sparkContext;

  public JavaSparkFacade(SparkConf sparkConf) {
    this.sparkContext = new JavaSparkContext(sparkConf);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <R, K, V> R createRDD(Class<? extends InputFormat> inputFormatClass,
                               Class<K> keyClass, Class<V> valueClass, Configuration hConf) {
    Configuration configuration = new Configuration(hConf);
    configuration.set(MRJobConfig.INPUT_FORMAT_CLASS_ATTR, inputFormatClass.getName());
    return (R) sparkContext.newAPIHadoopRDD(configuration, inputFormatClass, keyClass, valueClass);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <R, K, V> void saveAsDataset(R rdd, String datasetName, Map<String, String> arguments,
                                      Class<K> keyClass, Class<V> valueClass, Configuration hConf) {
    Preconditions.checkArgument(rdd instanceof JavaPairRDD,
                                "RDD class %s is not a subclass of %s",
                                rdd.getClass().getName(), JavaPairRDD.class.getName());

    Configuration configuration = new Configuration(hConf);
    SparkDatasetOutputFormat.setDataset(configuration, datasetName, arguments);
    configuration.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, SparkDatasetOutputFormat.class.getName());
    ((JavaPairRDD<K, V>) rdd).saveAsNewAPIHadoopDataset(configuration);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getContext() {
    return (T) sparkContext;
  }

  @Override
  public void stop() {
    sparkContext.stop();
  }
}
