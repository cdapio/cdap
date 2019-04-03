/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.api.common;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Test RuntimeArguments class.
 */
public class RuntimeArgumentsTest {

  @Test
  public void testMaptoString() {
    Map<String, String> testMap = new LinkedHashMap<>();
    testMap.put("key1", "val1");
    testMap.put("key2", "val2");
    testMap.put("keyOnly", "");

    String [] expectedArray = new String[]{"--key1=val1", "--key2=val2", "--keyOnly"};
    Assert.assertArrayEquals(expectedArray, RuntimeArguments.toPosixArray(testMap));
    Assert.assertArrayEquals(expectedArray, RuntimeArguments.toPosixArray(testMap.entrySet()));
  }

  @Test
  public void testStringtoMap() {
    String[] argArray = new String[] { "--key1=val1" , "--key2=val2", "--keyOnly"};
    Map<String, String> expectedMap = new HashMap<>();
    expectedMap.put("key1", "val1");
    expectedMap.put("key2", "val2");
    expectedMap.put("keyOnly", "");
    Assert.assertEquals(expectedMap, RuntimeArguments.fromPosixArray(argArray));
  }

  @Test
  public void testScopedArguments() {
    Map<String, String> runtimeArguments = new HashMap<>();

    runtimeArguments.put("debug", "true");
    runtimeArguments.put("mapreduce.*.debug", "false");
    runtimeArguments.put("mapreduce.OneMR.debug", "true");

    runtimeArguments.put("input.path", "global.input.path");
    runtimeArguments.put("mapreduce.OneMR.input.path", "OneMR.input.path");
    runtimeArguments.put("mapreduce.AnotherMR.input.path", "AnotherMR.input.path");
    runtimeArguments.put("spark.*.input.path", "AllSpark.input.path");

    runtimeArguments.put("output.path", "global.output.path");
    runtimeArguments.put("mapreduce.OneMR.output.path", "OneMR.output.path");
    runtimeArguments.put("spark.AnotherSpark.output.path", "AnotherSpark.output.path");

    runtimeArguments.put("mapreduce.*.processing.time", "1HR");

    runtimeArguments.put("dataset.Purchase.cache.seconds", "30");
    runtimeArguments.put("dataset.UserProfile.schema.property", "constant");
    runtimeArguments.put("dataset.unknown.dataset", "false");
    runtimeArguments.put("dataset.*.read.timeout", "60");

    Map<String, String> oneMRArguments = RuntimeArguments.extractScope(Scope.MAPREDUCE, "OneMR", runtimeArguments);
    Assert.assertEquals(16, oneMRArguments.size());
    Assert.assertEquals("true", oneMRArguments.get("debug"));
    Assert.assertEquals("false", oneMRArguments.get("mapreduce.*.debug"));
    Assert.assertEquals("true", oneMRArguments.get("mapreduce.OneMR.debug"));

    Assert.assertEquals("OneMR.input.path", oneMRArguments.get("input.path"));
    Assert.assertEquals("OneMR.input.path", oneMRArguments.get("mapreduce.OneMR.input.path"));
    Assert.assertEquals("AnotherMR.input.path", oneMRArguments.get("mapreduce.AnotherMR.input.path"));
    Assert.assertEquals("AllSpark.input.path", oneMRArguments.get("spark.*.input.path"));

    Assert.assertEquals("OneMR.output.path", oneMRArguments.get("output.path"));
    Assert.assertEquals("OneMR.output.path", oneMRArguments.get("mapreduce.OneMR.output.path"));
    Assert.assertEquals("AnotherSpark.output.path", oneMRArguments.get("spark.AnotherSpark.output.path"));

    Assert.assertEquals("1HR", oneMRArguments.get("processing.time"));
    Assert.assertEquals("1HR", oneMRArguments.get("mapreduce.*.processing.time"));

    Assert.assertEquals("30", oneMRArguments.get("dataset.Purchase.cache.seconds"));
    Assert.assertEquals("constant", oneMRArguments.get("dataset.UserProfile.schema.property"));
    Assert.assertEquals("false", oneMRArguments.get("dataset.unknown.dataset"));
    Assert.assertEquals("60", oneMRArguments.get("dataset.*.read.timeout"));

    Map<String, String> anotherMRArguments = RuntimeArguments.extractScope("mapreduce", "AnotherMR", runtimeArguments);
    Assert.assertEquals(16, anotherMRArguments.size());
    Assert.assertEquals("false", anotherMRArguments.get("debug"));
    Assert.assertEquals("false", anotherMRArguments.get("mapreduce.*.debug"));
    Assert.assertEquals("true", anotherMRArguments.get("mapreduce.OneMR.debug"));

    Assert.assertEquals("AnotherMR.input.path", anotherMRArguments.get("input.path"));
    Assert.assertEquals("OneMR.input.path", anotherMRArguments.get("mapreduce.OneMR.input.path"));
    Assert.assertEquals("AnotherMR.input.path", anotherMRArguments.get("mapreduce.AnotherMR.input.path"));
    Assert.assertEquals("AllSpark.input.path", anotherMRArguments.get("spark.*.input.path"));

    Assert.assertEquals("global.output.path", anotherMRArguments.get("output.path"));
    Assert.assertEquals("OneMR.output.path", anotherMRArguments.get("mapreduce.OneMR.output.path"));
    Assert.assertEquals("AnotherSpark.output.path", anotherMRArguments.get("spark.AnotherSpark.output.path"));

    Assert.assertEquals("1HR", anotherMRArguments.get("processing.time"));
    Assert.assertEquals("1HR", anotherMRArguments.get("mapreduce.*.processing.time"));

    Assert.assertEquals("30", anotherMRArguments.get("dataset.Purchase.cache.seconds"));
    Assert.assertEquals("constant", anotherMRArguments.get("dataset.UserProfile.schema.property"));
    Assert.assertEquals("false", anotherMRArguments.get("dataset.unknown.dataset"));
    Assert.assertEquals("60", anotherMRArguments.get("dataset.*.read.timeout"));

    Map<String, String> oneSparkArguments = RuntimeArguments.extractScope(Scope.SPARK, "OneSpark", runtimeArguments);
    Assert.assertEquals(15, oneSparkArguments.size());
    Assert.assertEquals("true", oneSparkArguments.get("debug"));
    Assert.assertEquals("false", oneSparkArguments.get("mapreduce.*.debug"));
    Assert.assertEquals("true", oneSparkArguments.get("mapreduce.OneMR.debug"));

    Assert.assertEquals("AllSpark.input.path", oneSparkArguments.get("input.path"));
    Assert.assertEquals("OneMR.input.path", oneSparkArguments.get("mapreduce.OneMR.input.path"));
    Assert.assertEquals("AnotherMR.input.path", oneSparkArguments.get("mapreduce.AnotherMR.input.path"));
    Assert.assertEquals("AllSpark.input.path", oneSparkArguments.get("spark.*.input.path"));

    Assert.assertEquals("global.output.path", oneSparkArguments.get("output.path"));
    Assert.assertEquals("OneMR.output.path", oneSparkArguments.get("mapreduce.OneMR.output.path"));
    Assert.assertEquals("AnotherSpark.output.path", oneSparkArguments.get("spark.AnotherSpark.output.path"));

    Assert.assertEquals("1HR", oneSparkArguments.get("mapreduce.*.processing.time"));

    Assert.assertEquals("30", oneSparkArguments.get("dataset.Purchase.cache.seconds"));
    Assert.assertEquals("constant", oneSparkArguments.get("dataset.UserProfile.schema.property"));
    Assert.assertEquals("false", oneSparkArguments.get("dataset.unknown.dataset"));
    Assert.assertEquals("60", oneSparkArguments.get("dataset.*.read.timeout"));

    Map<String, String> anotherSparkArguments = RuntimeArguments.extractScope("spark",
                                                                              "AnotherSpark", runtimeArguments);
    Assert.assertEquals(15, anotherSparkArguments.size());
    Assert.assertEquals("true", anotherSparkArguments.get("debug"));
    Assert.assertEquals("false", anotherSparkArguments.get("mapreduce.*.debug"));
    Assert.assertEquals("true", anotherSparkArguments.get("mapreduce.OneMR.debug"));

    Assert.assertEquals("AllSpark.input.path", anotherSparkArguments.get("input.path"));
    Assert.assertEquals("OneMR.input.path", anotherSparkArguments.get("mapreduce.OneMR.input.path"));
    Assert.assertEquals("AnotherMR.input.path", anotherSparkArguments.get("mapreduce.AnotherMR.input.path"));
    Assert.assertEquals("AllSpark.input.path", anotherSparkArguments.get("spark.*.input.path"));

    Assert.assertEquals("AnotherSpark.output.path", anotherSparkArguments.get("output.path"));
    Assert.assertEquals("OneMR.output.path", anotherSparkArguments.get("mapreduce.OneMR.output.path"));
    Assert.assertEquals("AnotherSpark.output.path", anotherSparkArguments.get("spark.AnotherSpark.output.path"));

    Assert.assertEquals("1HR", anotherSparkArguments.get("mapreduce.*.processing.time"));

    Assert.assertEquals("30", anotherSparkArguments.get("dataset.Purchase.cache.seconds"));
    Assert.assertEquals("constant", anotherSparkArguments.get("dataset.UserProfile.schema.property"));
    Assert.assertEquals("false", anotherSparkArguments.get("dataset.unknown.dataset"));
    Assert.assertEquals("60", anotherSparkArguments.get("dataset.*.read.timeout"));

    Map<String, String> purchaseArguments = RuntimeArguments.extractScope(Scope.DATASET, "Purchase", oneMRArguments);
    Assert.assertEquals(18, purchaseArguments.size());
    Assert.assertEquals("true", purchaseArguments.get("debug"));
    Assert.assertEquals("false", purchaseArguments.get("mapreduce.*.debug"));
    Assert.assertEquals("true", purchaseArguments.get("mapreduce.OneMR.debug"));

    Assert.assertEquals("OneMR.input.path", purchaseArguments.get("input.path"));
    Assert.assertEquals("OneMR.input.path", purchaseArguments.get("mapreduce.OneMR.input.path"));
    Assert.assertEquals("AnotherMR.input.path", purchaseArguments.get("mapreduce.AnotherMR.input.path"));
    Assert.assertEquals("AllSpark.input.path", purchaseArguments.get("spark.*.input.path"));

    Assert.assertEquals("OneMR.output.path", purchaseArguments.get("output.path"));
    Assert.assertEquals("OneMR.output.path", purchaseArguments.get("mapreduce.OneMR.output.path"));
    Assert.assertEquals("AnotherSpark.output.path", purchaseArguments.get("spark.AnotherSpark.output.path"));

    Assert.assertEquals("1HR", purchaseArguments.get("processing.time"));
    Assert.assertEquals("1HR", purchaseArguments.get("mapreduce.*.processing.time"));

    Assert.assertEquals("30", purchaseArguments.get("dataset.Purchase.cache.seconds"));
    Assert.assertEquals("constant", purchaseArguments.get("dataset.UserProfile.schema.property"));
    Assert.assertEquals("false", purchaseArguments.get("dataset.unknown.dataset"));
    Assert.assertEquals("60", purchaseArguments.get("dataset.*.read.timeout"));
    Assert.assertEquals("30", purchaseArguments.get("cache.seconds"));
    Assert.assertEquals("60", purchaseArguments.get("read.timeout"));

    Map<String, String> userprofileArguments = RuntimeArguments.extractScope(Scope.DATASET, "UserProfile",
                                                                             oneMRArguments);
    Assert.assertEquals(18, userprofileArguments.size());
    Assert.assertEquals("true", userprofileArguments.get("debug"));
    Assert.assertEquals("false", userprofileArguments.get("mapreduce.*.debug"));
    Assert.assertEquals("true", userprofileArguments.get("mapreduce.OneMR.debug"));

    Assert.assertEquals("OneMR.input.path", userprofileArguments.get("input.path"));
    Assert.assertEquals("OneMR.input.path", userprofileArguments.get("mapreduce.OneMR.input.path"));
    Assert.assertEquals("AnotherMR.input.path", userprofileArguments.get("mapreduce.AnotherMR.input.path"));
    Assert.assertEquals("AllSpark.input.path", userprofileArguments.get("spark.*.input.path"));

    Assert.assertEquals("OneMR.output.path", userprofileArguments.get("output.path"));
    Assert.assertEquals("OneMR.output.path", userprofileArguments.get("mapreduce.OneMR.output.path"));
    Assert.assertEquals("AnotherSpark.output.path", userprofileArguments.get("spark.AnotherSpark.output.path"));

    Assert.assertEquals("1HR", userprofileArguments.get("processing.time"));
    Assert.assertEquals("1HR", userprofileArguments.get("mapreduce.*.processing.time"));

    Assert.assertEquals("30", userprofileArguments.get("dataset.Purchase.cache.seconds"));
    Assert.assertEquals("constant", userprofileArguments.get("dataset.UserProfile.schema.property"));
    Assert.assertEquals("false", userprofileArguments.get("dataset.unknown.dataset"));
    Assert.assertEquals("60", userprofileArguments.get("dataset.*.read.timeout"));
    Assert.assertEquals("constant", userprofileArguments.get("schema.property"));
    Assert.assertEquals("60", userprofileArguments.get("read.timeout"));

    purchaseArguments = RuntimeArguments.extractScope(Scope.DATASET, "Purchase", oneSparkArguments);
    Assert.assertEquals(17, purchaseArguments.size());
    Assert.assertEquals("true", purchaseArguments.get("debug"));
    Assert.assertEquals("false", purchaseArguments.get("mapreduce.*.debug"));
    Assert.assertEquals("true", purchaseArguments.get("mapreduce.OneMR.debug"));

    Assert.assertEquals("AllSpark.input.path", purchaseArguments.get("input.path"));
    Assert.assertEquals("OneMR.input.path", purchaseArguments.get("mapreduce.OneMR.input.path"));
    Assert.assertEquals("AnotherMR.input.path", purchaseArguments.get("mapreduce.AnotherMR.input.path"));
    Assert.assertEquals("AllSpark.input.path", purchaseArguments.get("spark.*.input.path"));

    Assert.assertEquals("global.output.path", purchaseArguments.get("output.path"));
    Assert.assertEquals("OneMR.output.path", purchaseArguments.get("mapreduce.OneMR.output.path"));
    Assert.assertEquals("AnotherSpark.output.path", purchaseArguments.get("spark.AnotherSpark.output.path"));

    Assert.assertEquals("1HR", purchaseArguments.get("mapreduce.*.processing.time"));

    Assert.assertEquals("30", purchaseArguments.get("cache.seconds"));
    Assert.assertEquals("60", purchaseArguments.get("read.timeout"));
    Assert.assertEquals("30", purchaseArguments.get("dataset.Purchase.cache.seconds"));
    Assert.assertEquals("constant", purchaseArguments.get("dataset.UserProfile.schema.property"));
    Assert.assertEquals("false", purchaseArguments.get("dataset.unknown.dataset"));
    Assert.assertEquals("60", purchaseArguments.get("dataset.*.read.timeout"));

    userprofileArguments = RuntimeArguments.extractScope(Scope.DATASET, "UserProfile", anotherSparkArguments);
    Assert.assertEquals(17, userprofileArguments.size());
    Assert.assertEquals("true", userprofileArguments.get("debug"));
    Assert.assertEquals("false", userprofileArguments.get("mapreduce.*.debug"));
    Assert.assertEquals("true", userprofileArguments.get("mapreduce.OneMR.debug"));

    Assert.assertEquals("AllSpark.input.path", userprofileArguments.get("input.path"));
    Assert.assertEquals("OneMR.input.path", userprofileArguments.get("mapreduce.OneMR.input.path"));
    Assert.assertEquals("AnotherMR.input.path", userprofileArguments.get("mapreduce.AnotherMR.input.path"));
    Assert.assertEquals("AllSpark.input.path", userprofileArguments.get("spark.*.input.path"));

    Assert.assertEquals("AnotherSpark.output.path", userprofileArguments.get("output.path"));
    Assert.assertEquals("OneMR.output.path", userprofileArguments.get("mapreduce.OneMR.output.path"));
    Assert.assertEquals("AnotherSpark.output.path", userprofileArguments.get("spark.AnotherSpark.output.path"));

    Assert.assertEquals("1HR", userprofileArguments.get("mapreduce.*.processing.time"));

    Assert.assertEquals("constant", userprofileArguments.get("schema.property"));
    Assert.assertEquals("60", userprofileArguments.get("read.timeout"));
    Assert.assertEquals("30", userprofileArguments.get("dataset.Purchase.cache.seconds"));
    Assert.assertEquals("constant", userprofileArguments.get("dataset.UserProfile.schema.property"));
    Assert.assertEquals("false", userprofileArguments.get("dataset.unknown.dataset"));
    Assert.assertEquals("60", userprofileArguments.get("dataset.*.read.timeout"));
  }
}
