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

package co.cask.cdap.dq.test;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.dq.DataQualityWritable;
import co.cask.cdap.dq.functions.DiscreteValuesHistogram;
import co.cask.cdap.dq.functions.HistogramWithBucketing;
import co.cask.cdap.dq.functions.Mean;
import co.cask.cdap.dq.functions.StandardDeviation;
import co.cask.cdap.dq.functions.UniqueValues;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;

/**
 * Test cases for aggregation function logic. Disjoint from actual DataQuality map reduce
 */
public class BatchAggregationFunctionsTest {
  private static final Type TOKEN_TYPE_MAP_STRING_INTEGER = new TypeToken<Map<String, Integer>>() { }.getType();
  private static final Gson GSON = new Gson();

  @Test
  public void averageReturnAggregationTest() throws Exception {
    byte[] val1 = Bytes.toBytes(10.0);
    Mean mean = new Mean();
    Double averageVal = mean.deserialize(val1);
    Assert.assertEquals(10.0, averageVal, 0);
  }

  @Test
  public void standardDeviationReturnAggregationTest() throws Exception {
    byte[] val1 = Bytes.toBytes(10.0);
    StandardDeviation standardDeviation = new StandardDeviation();
    Double stdevVal = standardDeviation.deserialize(val1);
    Assert.assertEquals(10.0, stdevVal, 0);
  }

  @Test
  public void discreteValuesHistogramReturnAggregationTest() throws Exception {
    DiscreteValuesHistogram discreteValuesHistogram = new DiscreteValuesHistogram();

    Map<String, Integer> map1 = Maps.newHashMap();
    map1.put("a", 1);
    map1.put("b", 2);

    Map<String, Integer> map2 = Maps.newHashMap();
    map2.put("a", 2);
    map2.put("b", 3);

    Map<String, Integer> combinedMap = Maps.newHashMap();
    combinedMap.put("a", 3);
    combinedMap.put("b", 5);

    byte[] bytesMap1 = Bytes.toBytes(GSON.toJson(map1));
    byte[] bytesMap2 = Bytes.toBytes(GSON.toJson(map2));

    discreteValuesHistogram.combine(bytesMap1);
    discreteValuesHistogram.combine(bytesMap2);

    Map<String, Integer> histogramVal = discreteValuesHistogram.retrieveAggregation();

    Assert.assertEquals(combinedMap, histogramVal);
  }

  @Test
  public void averageGenerateAggregationTest() throws Exception {
    DataQualityWritable val1 = new DataQualityWritable();
    val1.set(new DoubleWritable(2.0));

    DataQualityWritable val2 = new DataQualityWritable();
    val2.set(new DoubleWritable(2.0));

    Mean mean = new Mean();
    mean.add(val1);
    mean.add(val2);

    byte[] output = mean.aggregate();
    Assert.assertEquals(2.0, Bytes.toDouble(output), 0);
  }

  @Test
  public void standardDeviationGenerateAggregationTest() throws Exception {
    DataQualityWritable val1 = new DataQualityWritable();
    val1.set(new DoubleWritable(2.0));

    DataQualityWritable val2 = new DataQualityWritable();
    val2.set(new DoubleWritable(5.0));

    DataQualityWritable val3 = new DataQualityWritable();
    val3.set(new DoubleWritable(10.0));

    DataQualityWritable val4 = new DataQualityWritable();
    val4.set(new DoubleWritable(52.0));

    StandardDeviation standardDeviation = new StandardDeviation();
    standardDeviation.add(val1);
    standardDeviation.add(val2);
    standardDeviation.add(val3);
    standardDeviation.add(val4);
    byte[] output = standardDeviation.aggregate();

    Assert.assertEquals(20.265426, Bytes.toDouble(output), 0.001);
  }

  @Test
  public void uniqueValuesGenerateAggregationTest() throws Exception {
    DataQualityWritable val1 = new DataQualityWritable();
    DataQualityWritable val2 = new DataQualityWritable();
    DataQualityWritable val3 = new DataQualityWritable();

    val1.set(new Text("a"));
    val2.set(new Text("a"));
    val3.set(new Text("a"));

    UniqueValues uniqueValues = new UniqueValues();
    uniqueValues.add(val1);
    uniqueValues.add(val2);
    uniqueValues.add(val3);
    byte[] output = uniqueValues.aggregate();

    Assert.assertEquals("[a]", Bytes.toString(output));
  }

  @Test
  public void discreteValuesGenerateAggregationTest() throws Exception {
    DataQualityWritable val1 = new DataQualityWritable();
    DataQualityWritable val2 = new DataQualityWritable();
    DataQualityWritable val3 = new DataQualityWritable();

    val1.set(new Text("a"));
    val2.set(new Text("a"));
    val3.set(new Text("b"));

    DiscreteValuesHistogram discreteValuesHistogram = new DiscreteValuesHistogram();
    discreteValuesHistogram.add(val1);
    discreteValuesHistogram.add(val2);
    discreteValuesHistogram.add(val3);

    Map<String, Integer> expectedMap = Maps.newHashMap();
    expectedMap.put("a", 2);
    expectedMap.put("b", 1);
    byte[] outputVal = discreteValuesHistogram.aggregate();
    Map<String, Integer> outputMap =
      GSON.fromJson(Bytes.toString(outputVal), TOKEN_TYPE_MAP_STRING_INTEGER);
    Assert.assertEquals(expectedMap, outputMap);
  }

  @Test
  public void histogramWithBucketingTest() throws Exception {
    DataQualityWritable val1 = new DataQualityWritable();
    DataQualityWritable val2 = new DataQualityWritable();
    DataQualityWritable val3 = new DataQualityWritable();
    DataQualityWritable val4 = new DataQualityWritable();
    DataQualityWritable val5 = new DataQualityWritable();
    DataQualityWritable val6 = new DataQualityWritable();
    DataQualityWritable val7 = new DataQualityWritable();
    DataQualityWritable val8 = new DataQualityWritable();

    val1.set(new IntWritable(2));
    val2.set(new IntWritable(3));
    val3.set(new IntWritable(4));
    val4.set(new IntWritable(16));
    val5.set(new IntWritable(16));
    val6.set(new IntWritable(26));
    val7.set(new IntWritable(46));
    val8.set(new IntWritable(56));

    HistogramWithBucketing histogramWithBucketing = new HistogramWithBucketing();
    histogramWithBucketing.add(val1);
    histogramWithBucketing.add(val2);
    histogramWithBucketing.add(val3);
    histogramWithBucketing.add(val4);
    histogramWithBucketing.add(val5);
    histogramWithBucketing.add(val6);
    histogramWithBucketing.add(val7);
    histogramWithBucketing.add(val8);

    histogramWithBucketing.aggregate();
    Map<Map.Entry<Double, Double>, Long> expectedMap = new HashMap<>();
    Map.Entry<Double, Double> expectedMapEntry = new AbstractMap.SimpleEntry<>(2.0, 86.0);
    expectedMap.put(expectedMapEntry, new Long(8));

    Assert.assertEquals(histogramWithBucketing.histogram, expectedMap);
  }
}

