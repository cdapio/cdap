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
import java.util.HashMap;
import java.util.Map;

/**
 * Discrete Values Histogram Aggregation Function
 */
public class DiscreteValuesHistogram
  implements BasicAggregationFunction, CombinableAggregationFunction<Map<String, Integer>> {
  private static final Gson GSON = new Gson();
  private static final Type TOKEN_TYPE_MAP_STRING_INTEGER = new TypeToken<Map<String, Integer>>() { }.getType();
  private Map<String, Integer> histogramMap = new HashMap<>();
  private Map<String, Integer> aggregatedHistogramMap = new HashMap<>();

  @Override
  public void combine(byte[] value) {
    Map<String, Integer> outputMap = deserialize(value);
    for (Map.Entry<String, Integer> entry : outputMap.entrySet()) {
      Integer val = aggregatedHistogramMap.get(entry.getKey());
      aggregatedHistogramMap.put(entry.getKey(), val == null ? entry.getValue() : val + entry.getValue());
    }
  }

  @Override
  public Map<String, Integer> deserialize(byte[] valueBytes) {
   return GSON.fromJson(Bytes.toString(valueBytes), TOKEN_TYPE_MAP_STRING_INTEGER);
  }

  @Override
  public Map<String, Integer> retrieveAggregation() {
    return aggregatedHistogramMap.isEmpty() ? null : aggregatedHistogramMap;
  }

  @Override
  public void add(DataQualityWritable value) {
    Integer mapVal = histogramMap.get(value.get().toString());
    if (mapVal != null) {
      histogramMap.put(value.get().toString(), mapVal + 1);
    } else {
      histogramMap.put(value.get().toString(), 1);
    }
  }

  @Override
  public byte[] aggregate() {
    return Bytes.toBytes(GSON.toJson(histogramMap));
  }
}
