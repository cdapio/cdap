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

package io.cdap.cdap.etl.common;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldReadOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldWriteOperation;
import io.cdap.cdap.etl.api.lineage.field.OperationType;

import java.lang.reflect.Type;

/**
 * Type adapter for {@link FieldOperation}.
 */
public class FieldOperationTypeAdapter implements JsonSerializer<FieldOperation>,
  JsonDeserializer<FieldOperation> {
  @Override
  public JsonElement serialize(FieldOperation src, Type typeOfSrc, JsonSerializationContext context) {
    return context.serialize(src);
  }

  @Override
  public FieldOperation deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
          throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    OperationType type = context.deserialize(jsonObj.get("type"), OperationType.class);

    switch (type) {
      case READ:
        return context.deserialize(json, FieldReadOperation.class);
      case TRANSFORM:
        return context.deserialize(json, FieldTransformOperation.class);
      case WRITE:
        return context.deserialize(json, FieldWriteOperation.class);
    }
    return null;
  }
}
