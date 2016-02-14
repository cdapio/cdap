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

import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;

import java.lang.reflect.Type;

/**
 * Codec for {@link co.cask.cdap.proto.Id.NamespacedId}. Currently only supports 
 * {@link co.cask.cdap.proto.Id.Application}, {@link co.cask.cdap.proto.Id.Artifact},
 * {@link co.cask.cdap.proto.Id.Program}, {@link co.cask.cdap.proto.Id.DatasetInstance}, 
 * {@link co.cask.cdap.proto.Id.Stream} and {@link co.cask.cdap.proto.Id.Stream.View}.
 * Support for other {@link co.cask.cdap.proto.Id.NamespacedId} objects will be added later.
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
      case "flow":
        return deserializeFlowId(id);
      case "flowlet":
        return deserializeFlowletId(id);
      case "service":
        return deserializeServiceId(id);
      case "schedule":
        return deserializeSchedule(id);
      case "worker":
        return deserializeWorkerId(id);
      case "workflow":
        return deserializeWorkflowId(id);
      case "datasetinstance":
        return deserializeDatasetInstanceId(id);
      case "stream":
        return deserializeStreamId(id);
      case "view":
        return deserializeViewId(id);
      case "artifact":
        return deserializeArtifactId(id);
      default:
        throw new UnsupportedOperationException(
          String.format("Unsupported object of type %s found. Deserialization of only %s, %s, %s, %s, %s, %s, %s, " +
                          "%s, %s, %s, %s, %s is supported.",
                        type,
                        Id.Application.class.getSimpleName(),
                        Id.Program.class.getSimpleName(),
                        Id.Flow.class.getSimpleName(),
                        Id.Flow.Flowlet.class.getSimpleName(),
                        Id.Service.class.getSimpleName(),
                        Id.Schedule.class.getSimpleName(),
                        Id.Worker.class.getSimpleName(),
                        Id.Workflow.class.getSimpleName(),
                        Id.DatasetInstance.class.getSimpleName(),
                        Id.Stream.class.getSimpleName(),
                        Id.Stream.View.class.getSimpleName(),
                        Id.Artifact.class.getSimpleName()
          )
        );
    }
  }

  private Id.Application deserializeApplicationId(JsonObject id) {
    Id.Namespace namespace = deserializeNamespace(id);
    String applicationId = id.get("applicationId").getAsString();
    return Id.Application.from(namespace, applicationId);
  }

  private Id.Artifact deserializeArtifactId(JsonObject id) {
    Id.Namespace namespace = deserializeNamespace(id);
    String artifactName = id.get("name").getAsString();
    ArtifactVersion artifactVersion = new ArtifactVersion(
      id.get("version").getAsJsonObject().get("version").getAsString());
    return Id.Artifact.from(namespace, artifactName, artifactVersion);
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

  private Id.Flow deserializeFlowId(JsonObject id) {
    Id.Program program = deserializeProgramId(id);
    return Id.Flow.from(program.getApplication(), program.getId());
  }

  private Id.Flow.Flowlet deserializeFlowletId(JsonObject id) {
    Id.Flow flow = deserializeFlowId(id.getAsJsonObject("flow"));
    String flowletId = id.get("id").getAsString();
    return Id.Flow.Flowlet.from(flow, flowletId);
  }

  private Id.Service deserializeServiceId(JsonObject id) {
    Id.Program program = deserializeProgramId(id);
    return Id.Service.from(program.getApplication(), program.getId());
  }

  private Id.Schedule deserializeSchedule(JsonObject id) {
    Id.Application app = deserializeApplicationId(id.getAsJsonObject("application"));
    String scheduleId = id.get("id").getAsString();
    return Id.Schedule.from(app, scheduleId);
  }

  private Id.Worker deserializeWorkerId(JsonObject id) {
    Id.Program program = deserializeProgramId(id);
    return Id.Worker.from(program.getApplication(), program.getId());
  }

  private Id.Workflow deserializeWorkflowId(JsonObject id) {
    Id.Program program = deserializeProgramId(id);
    return Id.Workflow.from(program.getApplication(), program.getId());
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

  private Id.Stream.View deserializeViewId(JsonObject id) {
    Id.Stream streamId = deserializeStreamId(id.getAsJsonObject("stream"));
    String view = id.get("id").getAsString();
    return Id.Stream.View.from(streamId, view);
  }
}
