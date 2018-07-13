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

package co.cask.cdap.etl.common;

import co.cask.cdap.etl.api.lineage.field.OperationType;
import co.cask.cdap.etl.api.lineage.field.PipelineOperation;
import co.cask.cdap.etl.api.lineage.field.PipelineReadOperation;
import co.cask.cdap.etl.api.lineage.field.PipelineTransformOperation;
import co.cask.cdap.etl.api.lineage.field.PipelineWriteOperation;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 * Type adapter for {@link PipelineOperation}.
 */
public class PipelineOperationTypeAdapter implements JsonSerializer<PipelineOperation>,
  JsonDeserializer<PipelineOperation> {
  @Override
  public JsonElement serialize(PipelineOperation src, Type typeOfSrc, JsonSerializationContext context) {
    return context.serialize(src);
  }

  @Override
  public PipelineOperation deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
          throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    OperationType type = context.deserialize(jsonObj.get("type"), OperationType.class);

    switch (type) {
      case READ:
        return context.deserialize(json, PipelineReadOperation.class);
      case TRANSFORM:
        return context.deserialize(json, PipelineTransformOperation.class);
      case WRITE:
        return context.deserialize(json, PipelineWriteOperation.class);
    }
    return null;
  }
}
