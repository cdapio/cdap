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
import java.util.HashSet;
import java.util.Set;

/**
 * Unique Values Aggregation Function
 */
public class UniqueValues implements BasicAggregationFunction, CombinableAggregationFunction<Set<String>> {
  private static final Gson GSON = new Gson();
  private static final Type TOKEN_TYPE_SET_STRING = new TypeToken<Set<String>>() { }.getType();
  private Set<String> uniqueValuesSet = new HashSet<>();
  private Set<String> aggregatedUniqueValuesSet = new HashSet<>();

  @Override
  public Set<String> retrieveAggregation() {
    return aggregatedUniqueValuesSet.isEmpty() ? null : aggregatedUniqueValuesSet;
  }

  @Override
  public Set<String> deserialize(byte[] valueBytes) {
    return GSON.fromJson(Bytes.toString(valueBytes), TOKEN_TYPE_SET_STRING);
  }

  @Override
  public void combine(byte[] value) {
    Set<String> outputSet = deserialize(value);
    aggregatedUniqueValuesSet.addAll(outputSet);
  }

  @Override
  public void add(DataQualityWritable value) {
    String val = value.get().toString();
    if (val != null) {
      uniqueValuesSet.add(val);
    }
  }

  @Override
  public byte[] aggregate() {
    return Bytes.toBytes(uniqueValuesSet.toString());
  }
}
