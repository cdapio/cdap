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

import io.cdap.cdap.api.data.batch.InputFormatProvider;
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
 * Unit tests for {@link RDDUtils}.
 */
public class RDDUtilsTest {

  private static JavaSparkContext jsc;

  @BeforeClass
  public static void setUpClass() {
    SparkConf conf = new SparkConf()
        .setMaster("local[1]")
        .setAppName("RDDUtilsTest")
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
  public void testReadUsingInputFormatSuccess() {
    InputFormatProvider validProvider = new InputFormatProvider() {
      @Override
      public String getInputFormatClassName() {
        return TextInputFormat.class.getName();
      }

      @Override
      public Map<String, String> getInputFormatConfiguration() {
        return Collections.singletonMap("mapreduce.input.fileinputformat.inputdir", "/tmp/in");
      }
    };

    JavaPairRDD<LongWritable, Text> rdd = RDDUtils.readUsingInputFormat(
        jsc, validProvider, getClass().getClassLoader(), LongWritable.class, Text.class);
    Assert.assertNotNull(rdd);
  }

  @Test
  public void testReadUsingInputFormatPropagatesClassNotFoundException() {
    InputFormatProvider invalidProvider = new InputFormatProvider() {
      @Override
      public String getInputFormatClassName() {
        return "non.existent.MissingInputFormat";
      }

      @Override
      public Map<String, String> getInputFormatConfiguration() {
        return Collections.emptyMap();
      }
    };

    try {
      RDDUtils.readUsingInputFormat(
          jsc, invalidProvider, getClass().getClassLoader(), LongWritable.class, Text.class);
      Assert.fail("Expected RuntimeException wrapping ClassNotFoundException");
    } catch (RuntimeException actual) {
      Assert.assertTrue(actual.getCause() instanceof ClassNotFoundException);
    }
  }
}
