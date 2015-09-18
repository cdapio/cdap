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

package co.cask.cdap.proto.codec;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Joiner;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;

import java.lang.reflect.Type;

/**
 * Codec for {@link Id.NamespacedId}. Currently only supports {@link Id.Application}, {@link Id.Program},
 * {@link Id.DatasetInstance} and {@link Id.Stream}. Support for other {@link Id.NamespacedId} objects will be added
 * later.
 */
public class NamespacedIdCodec extends AbstractSpecificationCodec<Id.NamespacedId> {
  @Override
  public JsonElement serialize(Id.NamespacedId src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();
    jsonObj.add("type", new JsonPrimitive(src.getIdType()));
    jsonObj.add("id", context.serialize(src));

    return jsonObj;
  }

  @Override
  public Id.NamespacedId deserialize(JsonElement json, Type typeOfT,
                                     JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();
    JsonObject id = jsonObj.getAsJsonObject("id");
    String type = jsonObj.get("type").getAsString();
    switch (type) {
      case "application":
        return deserializeApplicationId(id);
      case "program":
        return deserializeProgramId(id);
      case "flowlet":
        return deserializeFlowletId(id);
      case "schedule":
        return deserializeSchedule(id);
      case "datasetinstance":
        return deserializeDatasetInstanceId(id);
      case "stream":
        return deserializeStreamId(id);
      default:
        throw new UnsupportedOperationException(
          String.format("Unsupported object of type %s found. Deserialization of only %s is supported.",
                        type,
                        Joiner.on(", ").join(
                          Id.Application.class.getSimpleName(),
                          Id.Program.class.getSimpleName(),
                          Id.Flowlet.class.getSimpleName(),
                          Id.Schedule.class.getSimpleName(),
                          Id.DatasetInstance.class.getSimpleName(),
                          Id.Stream.class.getSimpleName())
          )
        );
    }
  }

  private Id.Application deserializeApplicationId(JsonObject id) {
    Id.Namespace namespace = deserializeNamespace(id);
    String applicationId = id.get("applicationId").getAsString();
    return Id.Application.from(namespace, applicationId);
  }

  private Id.Namespace deserializeNamespace(JsonObject id) {
    String namespace = id.getAsJsonObject("namespace").get("id").getAsString();
    return Id.Namespace.from(namespace);
  }

  private Id.Program deserializeProgramId(JsonObject id) {
    Id.Application app = deserializeApplicationId(id.getAsJsonObject("application"));
    ProgramType programType = ProgramType.valueOf(id.get("type").getAsString().toUpperCase());
    String programId = id.get("id").getAsString();
    return Id.Program.from(app, programType, programId);
  }

  private Id.Flowlet deserializeFlowletId(JsonObject id) {
    Id.Program flow = deserializeProgramId(id.getAsJsonObject("flow"));
    String flowletId = id.get("id").getAsString();
    return Id.Flowlet.from(flow, flowletId);
  }

  private Id.Schedule deserializeSchedule(JsonObject id) {
    Id.Application app = deserializeApplicationId(id.getAsJsonObject("application"));
    String scheduleId = id.get("id").getAsString();
    return Id.Schedule.from(app, scheduleId);
  }

  private Id.DatasetInstance deserializeDatasetInstanceId(JsonObject id) {
    Id.Namespace namespace = deserializeNamespace(id);
    String instanceId = id.get("instanceId").getAsString();
    return Id.DatasetInstance.from(namespace, instanceId);
  }

  private Id.Stream deserializeStreamId(JsonObject id) {
    Id.Namespace namespace = deserializeNamespace(id);
    String streamName = id.get("streamName").getAsString();
    return Id.Stream.from(namespace, streamName);
  }
}
