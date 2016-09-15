/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.spark.batch;

import co.cask.cdap.api.data.batch.Split;
import com.google.common.base.Objects;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * Type adapter for {@link DatasetInfo}
 */
public class DatasetInfoTypeAdapter implements JsonSerializer<DatasetInfo>, JsonDeserializer<DatasetInfo> {
  private static final Type mapType = new TypeToken<Map<String, String>>() { }.getType();

  @Override
  public DatasetInfo deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject obj = json.getAsJsonObject();
    String datasetName = obj.get("datasetName").getAsString();
    Map<String, String> datasetArgs = context.deserialize(obj.get("datasetArgs"), mapType);
    if (obj.get("datasetSplitClass") == null) {
      return new DatasetInfo(datasetName, datasetArgs, null);
    }
    String datasetSplitClass = obj.get("datasetSplitClass").getAsString();
    ClassLoader classLoader = Objects.firstNonNull(Thread.currentThread().getContextClassLoader(),
                                                   SparkBatchSourceFactory.class.getClassLoader());
    try {
      Class<?> splitClass = classLoader.loadClass(datasetSplitClass);
      List<Split> splits = context.deserialize(obj.get("datasetSplits"), getListType(splitClass));
      return new DatasetInfo(datasetName, datasetArgs, splits);
    } catch (ClassNotFoundException e) {
      throw new JsonParseException("Unable to deserialize splits", e);
    }
  }

  @Override
  public JsonElement serialize(DatasetInfo src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();
    jsonObj.addProperty("datasetName", src.getDatasetName());
    jsonObj.add("datasetArgs", context.serialize(src.getDatasetArgs()));
    if (src.getSplits() != null && !src.getSplits().isEmpty()) {
      jsonObj.addProperty("datasetSplitClass", src.getSplits().get(0).getClass().getName());
      jsonObj.add("datasetSplits", context.serialize(src.getSplits()));
    }
    return jsonObj;
  }

  private static <T> Type getListType(Class<T> elementType) {
    return new TypeToken<List<T>>() { }.where(new TypeParameter<T>() { }, elementType).getType();
  }
}
