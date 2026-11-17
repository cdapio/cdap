/*
 * Copyright © 2023 Cask Data, Inc.
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

import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for {@link SparkBatchSourceFactory}.
 */
public class SparkBatchSourceFactoryTest {

  private static JavaSparkContext jsc;

  @BeforeClass
  public static void setUpClass() {
    SparkConf conf = new SparkConf()
        .setMaster("local[1]")
        .setAppName("SparkBatchSourceFactoryTest")
        .set("spark.ui.enabled", "false");
    jsc = new JavaSparkContext(conf);
  }

  @AfterClass
  public static void tearDownClass() {
    if (jsc != null) {
      jsc.close();
      jsc = null;
    }
  }

  @Test
  public void testCreateRddWithContextAndFallbackClassLoader() {
    InputFormatProvider provider = new InputFormatProvider() {
      @Override
      public String getInputFormatClassName() {
        return TextInputFormat.class.getName();
      }

      @Override
      public Map<String, String> getInputFormatConfiguration() {
        return Collections.singletonMap("mapreduce.input.fileinputformat.inputdir", "/tmp/src");
      }
    };

    SparkBatchSourceFactory factory = new SparkBatchSourceFactory();
    factory.addInput("sourceStage", Input.of("sourceInput", provider).alias("alias1"));
    JavaSparkExecutionContext sec =
        new BatchSQLEngineAdapterTest().new MockJavaSparkExecutionContext();

    JavaPairRDD<LongWritable, Text> rddWithContextCl =
        factory.createRDD(sec, jsc, "sourceStage", LongWritable.class, Text.class);
    Assert.assertNotNull(rddWithContextCl);

    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(null);
    try {
      JavaPairRDD<LongWritable, Text> rddWithFallbackCl =
          factory.createRDD(sec, jsc, "sourceStage", LongWritable.class, Text.class);
      Assert.assertNotNull(rddWithFallbackCl);
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }
}
