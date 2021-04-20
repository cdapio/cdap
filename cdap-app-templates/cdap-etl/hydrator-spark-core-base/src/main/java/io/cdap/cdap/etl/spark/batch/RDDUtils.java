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

package io.cdap.cdap.etl.spark.batch;

import com.google.common.base.Throwables;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Common RDD operations
 */
public class RDDUtils {
  private static final Logger LOG = LoggerFactory.getLogger(RDDUtils.class);

  public static void saveUsingOutputFormat(OutputFormatProvider outputFormatProvider, JavaPairRDD<?, ?> rdd) {
    Configuration hConf = new Configuration();
    for (Map.Entry<String, String> entry : outputFormatProvider.getOutputFormatConfiguration().entrySet()) {
      hConf.set(entry.getKey(), entry.getValue());
    }
    hConf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, outputFormatProvider.getOutputFormatClassName());
    saveHadoopDataset(rdd, hConf);
  }

  public static <K, V> void saveHadoopDataset(JavaPairRDD<K, V> rdd, Configuration hConf) {
    // Spark expects the conf to be the job configuration, and to contain the credentials
    JobConf jobConf = new JobConf(hConf);
    try {
      jobConf.setCredentials(UserGroupInformation.getCurrentUser().getCredentials());
    } catch (IOException e) {
      LOG.warn("Failed to get the current UGI. Continue to execute without user credentials.", e);
    }
    rdd.saveAsNewAPIHadoopDataset(jobConf);
  }

  @SuppressWarnings("unchecked")
  public static <K, V> JavaPairRDD<K, V> readUsingInputFormat(JavaSparkContext jsc,
                                                              InputFormatProvider inputFormatProvider,
                                                              ClassLoader classLoader,
                                                              Class<K> keyClass, Class<V> valueClass) {
    Configuration hConf = new Configuration();
    hConf.clear();
    for (Map.Entry<String, String> entry : inputFormatProvider.getInputFormatConfiguration().entrySet()) {
      hConf.set(entry.getKey(), entry.getValue());
    }
    try {
      @SuppressWarnings("unchecked")
      Class<InputFormat> inputFormatClass = (Class<InputFormat>) classLoader.loadClass(
        inputFormatProvider.getInputFormatClassName());
      return jsc.newAPIHadoopRDD(hConf, inputFormatClass, keyClass, valueClass);
    } catch (ClassNotFoundException e) {
      throw Throwables.propagate(e);
    }
  }

}
