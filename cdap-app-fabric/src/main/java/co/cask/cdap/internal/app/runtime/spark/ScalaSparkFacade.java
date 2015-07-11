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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

/**
 * Implements {@link SparkFacade} with a Scala {@link SparkContext}.
 */
final class ScalaSparkFacade implements SparkFacade {

  private final SparkContext sparkContext;

  public ScalaSparkFacade(SparkConf sparkConf) {
    this.sparkContext = new SparkContext(sparkConf);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <R, K, V> R createRDD(Class<? extends InputFormat> inputFormatClass,
                               Class<K> keyClass, Class<V> valueClass, Configuration hConf) {
    hConf.set(MRJobConfig.INPUT_FORMAT_CLASS_ATTR, inputFormatClass.getName());
    return (R) sparkContext.newAPIHadoopRDD(hConf, inputFormatClass, keyClass, valueClass);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <R, K, V> void saveAsDataset(R rdd, Class<? extends OutputFormat> outputFormatClass,
                                      Class<K> keyClass, Class<V> valueClass, Configuration hConf) {
    Preconditions.checkArgument(rdd instanceof RDD,
                                "RDD class %s is not a subclass of %s",
                                rdd.getClass().getName(), RDD.class.getName());

    hConf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, outputFormatClass.getName());

    ClassTag<K> kClassTag = ClassTag$.MODULE$.apply(keyClass);
    ClassTag<V> vClassTag = ClassTag$.MODULE$.apply(valueClass);

    PairRDDFunctions<K, V> pairRDD = new PairRDDFunctions<K, V>((RDD<Tuple2<K, V>>) rdd, kClassTag, vClassTag, null);
    pairRDD.saveAsNewAPIHadoopDataset(hConf);
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
