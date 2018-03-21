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
package co.cask.cdap.report.proto;

import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.report.util.ReportField;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import java.lang.reflect.Type;

/**
 * Deserializer for {@link ReportGenerationRequest.Filter}
 */
public class FilterDeserializer implements JsonDeserializer<ReportGenerationRequest.Filter> {
  private static final Type INT_RANGE_FILTER_TYPE =
    new TypeToken<ReportGenerationRequest.RangeFilter<Integer>>() { }.getType();
  private static final Type LONG_RANGE_FILTER_TYPE =
    new TypeToken<ReportGenerationRequest.RangeFilter<Long>>() { }.getType();
  private static final Type STRING_VALUE_FILTER_TYPE =
    new TypeToken<ReportGenerationRequest.ValueFilter<String>>() { }.getType();

  @Override
  public ReportGenerationRequest.Filter deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    if (json == null) {
      return null;
    }
    if (!(json instanceof JsonObject)) {
      throw new JsonParseException("Expected a JsonObject but found a " + json.getClass().getName());
    }

    JsonObject object = (JsonObject) json;
    JsonElement fieldName = object.get("fieldName");
    if (fieldName == null) {
      throw new JsonParseException("Field name must be specified for filters");
    }
    ReportField fieldType = ReportField.valueOfFieldName(fieldName.getAsString());
    if (fieldType == null) {
      throw new JsonParseException("Invalid field name " + fieldName);
    }
    if (object.get("range") != null) {
      if (!fieldType.getApplicableFilters().contains(ReportField.FilterType.RANGE)) {
        throw new JsonParseException("Field " + fieldName + " cannot be filtered by range");
      }
      if (fieldType.getValueClass().equals(Integer.class)) {
        return context.deserialize(json, INT_RANGE_FILTER_TYPE);
      }
      if (fieldType.getValueClass().equals(Long.class)) {
        return context.deserialize(json, LONG_RANGE_FILTER_TYPE);
      }
      throw new JsonParseException(String.format("Field %s with value type %s cannot be filtered by range", fieldName,
                                                 fieldType.getValueClass().getName()));
    }
    if (!fieldType.getApplicableFilters().contains(ReportField.FilterType.VALUE)) {
      throw new JsonParseException("Field " + fieldName + " cannot be filtered by values");
    }
    if (fieldType.getValueClass().equals(String.class)) {
      return context.deserialize(json, STRING_VALUE_FILTER_TYPE);
    }
    throw new JsonParseException(String.format("Field %s with value type %s cannot be filtered by values", fieldName,
                                               fieldType.getValueClass().getName()));
  }
}
