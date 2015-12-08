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

package co.cask.cdap.etl.batch.spark;

import co.cask.cdap.api.data.batch.Split;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Information required to read from or write to a dataset in the spark program.
 */
final class DatasetInfo {

  private final String datasetName;
  private final Map<String, String> datasetArgs;
  private final List<Split> splits;

  public static DatasetInfo deserialize(DataInput input) throws IOException {
    return new DatasetInfo(input.readUTF(),
                           Serializations.deserializeMap(input, Serializations.createStringObjectReader()),
                           deserializeSplits(input));
  }

  DatasetInfo(String datasetName, Map<String, String> datasetArgs, @Nullable List<Split> splits) {
    this.datasetName = datasetName;
    this.datasetArgs = ImmutableMap.copyOf(datasetArgs);
    this.splits = splits == null ? null : ImmutableList.copyOf(splits);
  }

  public String getDatasetName() {
    return datasetName;
  }

  public Map<String, String> getDatasetArgs() {
    return datasetArgs;
  }

  @Nullable
  public List<Split> getSplits() {
    return splits;
  }

  void serialize(DataOutput output) throws IOException {
    output.writeUTF(getDatasetName());
    Serializations.serializeMap(getDatasetArgs(), Serializations.createStringObjectWriter(), output);
    serializeSplits(getSplits(), output);
  }

  private static void serializeSplits(@Nullable List<Split> splits, DataOutput output) throws IOException {
    if (splits == null || splits.isEmpty()) {
      output.writeInt(0);
      return;
    }
    // A bit hacky since we grab the split class name from the first element.
    output.writeUTF(splits.get(0).getClass().getName());
    output.writeUTF(new Gson().toJson(splits));
  }

  @Nullable
  private static List<Split> deserializeSplits(DataInput input) throws IOException {
    int size = input.readInt();
    if (size == 0) {
      return null;
    }
    ClassLoader classLoader = Objects.firstNonNull(Thread.currentThread().getContextClassLoader(),
                                                   SparkBatchSourceFactory.class.getClassLoader());
    try {
      Class<?> splitClass = classLoader.loadClass(input.readUTF());
      return new Gson().fromJson(input.readUTF(), getListType(splitClass));
    } catch (ClassNotFoundException e) {
      throw new IOException("Unable to deserialize splits", e);
    }
  }

  private static <T> Type getListType(Class<T> elementType) {
    return new TypeToken<List<T>>() { }.where(new TypeParameter<T>() { }, elementType).getType();
  }
}
