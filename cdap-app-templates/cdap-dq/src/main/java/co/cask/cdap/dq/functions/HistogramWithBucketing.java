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

package co.cask.cdap.dq.functions;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.dq.DataQualityWritable;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Aggregation function creates a histogram with custom bucketing
 * for numbers - no categorical data
 */
public class HistogramWithBucketing implements BasicAggregationFunction<Map<Map.Entry<Double, Double>, Integer>> {
  private static final Gson GSON = new Gson();
  private static final Type TOKEN_TYPE_MAP_MAP_ENTRY_DOUBLE_DOUBLE_LONG =
    new TypeToken<Map<Map.Entry<Double, Double>, Long>>() { }.getType();

  private ArrayList<Double> values = new ArrayList<>();
  private Double max = Double.MIN_VALUE;
  private Double min = Double.MAX_VALUE;
  public Map<Map.Entry<Double, Double>, Long> histogram = new HashMap<>();

  @Override
  public void add(DataQualityWritable value) {
    Double newValue = Double.parseDouble(value.get().toString());
    max = newValue > max ? newValue : max;
    min = newValue < min ? newValue : min;
    values.add(newValue);
  }

  @Override
  public byte[] aggregate() {
    Bucketing bucketing = new Bucketing("automatic", null);
    bucketing.doBucketing();
    for (Double value : values) {
      for (Map.Entry<Map.Entry<Double, Double>, Long> bucketMapEntry : histogram.entrySet()) {
        if (value >= bucketMapEntry.getKey().getKey() && value <= bucketMapEntry.getKey().getValue()) {
          bucketMapEntry.setValue(bucketMapEntry.getValue() + 1);
          break;
        }
      }
    }
    String aggregationJSON = GSON.toJson(histogram);
    return Bytes.toBytes(aggregationJSON);
  }

  @Override
  public Map<Map.Entry<Double, Double>, Integer> deserialize(byte[] serializedValue) {
    String valueJSON = Bytes.toString(serializedValue);
    return GSON.fromJson(valueJSON, TOKEN_TYPE_MAP_MAP_ENTRY_DOUBLE_DOUBLE_LONG);
  }

  private class Bucketing {
    String bucketingStrategy;
    Integer maxBucketSize;
    private Bucketing(String bucketingStrategy, Integer maxBucketSize) {
      this.maxBucketSize = maxBucketSize == null ? 10 : maxBucketSize;
      this.bucketingStrategy = bucketingStrategy;
    }

    private void doBucketing() {
      if ("automatic".equals(bucketingStrategy)) {
        automaticallyGenerateBuckets();
      } else {
        if (maxBucketSize > 0) {
          manuallyGenerateBuckets(maxBucketSize);
        }
      }
    }

    /**
     * Generates buckets using the Freedman-Diaconis rule
     * Which says: Bin size = 2 * IQR(x) n^(-1/3)
     */
    private void automaticallyGenerateBuckets() {
      Collections.sort(values);
      long valuesListSize = (long) values.size();
      long quartile = (long) Math.floor(valuesListSize / 4.0);
      Double firstQuartile = values.get((int) quartile);
      Double thirdQuartile = values.get((int) quartile * 3);
      Double interquartileRange = thirdQuartile - firstQuartile;
      Long maxBucketSize = (long) Math.ceil(2 * interquartileRange * Math.pow(valuesListSize, -1 / 3));
      if (maxBucketSize == 0L) {
        maxBucketSize = 1L;
      }
      for (double i = min; i < max; i += maxBucketSize) {
        Map.Entry<Double, Double> mapEntry =
          new AbstractMap.SimpleEntry<>(i, i + maxBucketSize);
        histogram.put(mapEntry, 0L);
      }
    }

    /**
     * Generates buckets by simply allowing the max span of a
     * bucket to be maxBucketSize
     */
    private void manuallyGenerateBuckets(long maxBucketSize) {
      Collections.sort(values);
      for (double i = min; i < max; i += maxBucketSize) {
        Map.Entry<Double, Double> mapEntry =
          new AbstractMap.SimpleEntry<>(i, i + maxBucketSize);
        histogram.put(mapEntry, 0L);
      }
    }
  }
}

