/*
 * Copyright © 2015-2018 Cask Data, Inc.
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
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.FlowId;
import co.cask.cdap.proto.id.FlowletId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.proto.id.ServiceId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import co.cask.cdap.proto.id.WorkerId;
import co.cask.cdap.proto.id.WorkflowId;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;

import java.lang.reflect.Type;

/**
 * Codec for converting data written in NamespacedId format to {@link NamespacedEntityId}.
 */
public class NamespacedIdCodec extends AbstractSpecificationCodec<NamespacedEntityId> {

  @Override
  public JsonElement serialize(NamespacedEntityId src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();
    jsonObj.add("type", new JsonPrimitive(src.getEntityType().toString()));
    jsonObj.add("id", context.serialize(src));

    return jsonObj;
  }

  @Override
  public NamespacedEntityId deserialize(JsonElement json, Type typeOfT,
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
                        ApplicationId.class.getSimpleName(),
                        ProgramId.class.getSimpleName(),
                        FlowId.class.getSimpleName(),
                        FlowletId.class.getSimpleName(),
                        ServiceId.class.getSimpleName(),
                        ScheduleId.class.getSimpleName(),
                        WorkerId.class.getSimpleName(),
                        WorkflowId.class.getSimpleName(),
                        DatasetId.class.getSimpleName(),
                        StreamId.class.getSimpleName(),
                        StreamViewId.class.getSimpleName(),
                        ArtifactId.class.getSimpleName()
          )
        );
    }
  }

  private ApplicationId deserializeApplicationId(JsonObject id) {
    NamespaceId namespace = deserializeNamespace(id);
    String applicationId = id.get("applicationId").getAsString();
    return new ApplicationId(namespace.getNamespace(), applicationId);
  }

  private ArtifactId deserializeArtifactId(JsonObject id) {
    NamespaceId namespace = deserializeNamespace(id);
    String artifactName = id.get("name").getAsString();
    ArtifactVersion artifactVersion = new ArtifactVersion(id.get("version").getAsJsonObject()
                                                            .get("version").getAsString());
    return new ArtifactId(namespace.getNamespace(), artifactName, artifactVersion.getVersion());
  }

  private NamespaceId deserializeNamespace(JsonObject id) {
    String namespace = id.getAsJsonObject("namespace").get("id").getAsString();
    return new NamespaceId(namespace);
  }

  private ProgramId deserializeProgramId(JsonObject id) {
    ApplicationId app = deserializeApplicationId(id.getAsJsonObject("application"));
    ProgramType programType = ProgramType.valueOf(id.get("type").getAsString().toUpperCase());
    String programId = id.get("id").getAsString();
    return new ProgramId(app.getNamespace(), app.getApplication(), programType, programId);
  }

  private FlowId deserializeFlowId(JsonObject id) {
    ProgramId flow = deserializeProgramId(id);
    return new FlowId(flow.getParent(), id.get("flow").getAsString());
  }

  private FlowletId deserializeFlowletId(JsonObject id) {
    FlowId flow = deserializeFlowId(id.getAsJsonObject("flow"));
    String flowletId = id.get("id").getAsString();
    return new FlowletId(flow.getParent(), flow.getProgram(), flowletId);
  }

  private ServiceId deserializeServiceId(JsonObject id) {
    ProgramId program = deserializeProgramId(id);
    return new ServiceId(program.getParent(), program.getProgram());
  }

  private ScheduleId deserializeSchedule(JsonObject id) {
    ApplicationId app = deserializeApplicationId(id.getAsJsonObject("application"));
    String scheduleId = id.get("id").getAsString();
    return new ScheduleId(app.getNamespace(), app.getApplication(), app.getVersion(), scheduleId);
  }

  private WorkerId deserializeWorkerId(JsonObject id) {
    ProgramId program = deserializeProgramId(id);
    return new WorkerId(program.getParent(), program.getProgram());
  }

  private WorkflowId deserializeWorkflowId(JsonObject id) {
    ProgramId program = deserializeProgramId(id);
    return new WorkflowId(program.getParent(), program.getProgram());
  }

  private DatasetId deserializeDatasetInstanceId(JsonObject id) {
    NamespaceId namespace = deserializeNamespace(id);
    String instanceId = id.get("instanceId").getAsString();
    return new DatasetId(namespace.getNamespace(), instanceId);
  }

  private StreamId deserializeStreamId(JsonObject id) {
    NamespaceId namespace = deserializeNamespace(id);
    String streamName = id.get("streamName").getAsString();
    return new StreamId(namespace.getNamespace(), streamName);
  }

  private StreamViewId deserializeViewId(JsonObject id) {
    StreamId streamId = deserializeStreamId(id.getAsJsonObject("stream"));
    String view = id.get("id").getAsString();
    return new StreamViewId(streamId.getNamespace(), streamId.getStream(), view);
  }
}
