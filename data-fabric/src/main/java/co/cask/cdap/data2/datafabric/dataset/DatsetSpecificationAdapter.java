/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.data2.datafabric.dataset;

import co.cask.cdap.api.dataset.DatasetSpecification;
import com.continuuity.tephra.TxConstants;
import com.google.common.reflect.TypeToken;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Serializer for {@link co.cask.cdap.api.dataset.DatasetSpecification} class
 */
public class DatsetSpecificationAdapter implements JsonSerializer<DatasetSpecification> {
  @Override
  public JsonElement serialize(DatasetSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("name", src.getName());
    jsonObject.addProperty("type", src.getType());
    JsonObject properties = new JsonObject();
    for (Map.Entry<String, String> property : src.getProperties().entrySet()) {
      if (property.getKey().equals(TxConstants.PROPERTY_TTL)) {
        long value = TimeUnit.MILLISECONDS.toSeconds(Long.parseLong(property.getValue()));
        properties.addProperty(property.getKey(), value);
      } else {
        properties.addProperty(property.getKey(), property.getValue());
      }
    }
    jsonObject.add("properties", properties);
    Type specsType = new TypeToken<Map<String, DatasetSpecification>>() { }.getType();
    jsonObject.add("datasetSpecs", context.serialize(src.getSpecifications(), specsType));
    return jsonObject;
  }
}
