/*
 * Copyright © 2016-2017 Cask Data, Inc.
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
import com.google.gson.JsonSerializationContext;

import java.lang.reflect.Type;

/**
 * Codec for {@link co.cask.cdap.proto.id.NamespacedEntityId}.
 */
public class NamespacedEntityIdCodec extends AbstractSpecificationCodec<NamespacedEntityId> {

  @Override
  public NamespacedEntityId deserialize(JsonElement json, Type typeOfT,
                                        JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String entity = jsonObj.get("entity").getAsString();

    switch (entity.toLowerCase()) {
      case "namespace":
        return deserializeNamespace(jsonObj);
      case "application":
        return deserializeApplicationId(jsonObj);
      case "program":
        return deserializeProgramId(jsonObj);
      case "flow":
        return deserializeFlowId(jsonObj);
      case "flowlet":
        return deserializeFlowletId(jsonObj);
      case "service":
        return deserializeServiceId(jsonObj);
      case "schedule":
        return deserializeSchedule(jsonObj);
      case "worker":
        return deserializeWorkerId(jsonObj);
      case "workflow":
        return deserializeWorkflowId(jsonObj);
      case "dataset":
        return deserializeDatasetId(jsonObj);
      case "stream":
        return deserializeStreamId(jsonObj);
      case "stream_view":
        return deserializeViewId(jsonObj);
      case "artifact":
        return deserializeArtifactId(jsonObj);
      default:
        throw new UnsupportedOperationException(
          String.format("Unsupported object of entity %s found. Deserialization of only %s, %s, %s, %s, %s, %s, %s, " +
                          "%s, %s, %s, %s, %s is supported.",
                        entity,
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
    String applicationId = id.get("application").getAsString();
    String version = id.get("version").getAsString();
    return new ApplicationId(namespace.getNamespace(), applicationId, version);
  }

  private NamespaceId deserializeNamespace(JsonObject id) {
    String namespace = id.get("namespace").getAsString();
    return new NamespaceId(namespace);
  }

  private ProgramId deserializeProgramId(JsonObject id) {
    ApplicationId app = deserializeApplicationId(id);
    ProgramType programType = ProgramType.valueOf(id.get("type").getAsString().toUpperCase());
    String programId = id.get("program").getAsString();
    return new ProgramId(app.getNamespace(), app.getApplication(), programType, programId);
  }

  private FlowId deserializeFlowId(JsonObject id) {
    ApplicationId applicationId = deserializeApplicationId(id);
    return new FlowId(applicationId, id.get("flow").getAsString());
  }

  private FlowletId deserializeFlowletId(JsonObject id) {
    FlowId flow = deserializeFlowId(id);
    String flowletId = id.get("flowlet").getAsString();
    return new FlowletId(flow.getParent(), flow.getProgram(), flowletId);
  }

  private ServiceId deserializeServiceId(JsonObject id) {
    ProgramId program = deserializeProgramId(id);
    return new ServiceId(program.getParent(), program.getProgram());
  }

  private ScheduleId deserializeSchedule(JsonObject id) {
    ApplicationId app = deserializeApplicationId(id);
    String scheduleId = id.get("schedule").getAsString();
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

  private ArtifactId deserializeArtifactId(JsonObject id) {
    NamespaceId namespace = deserializeNamespace(id);
    String artifactName = id.get("artifact").getAsString();
    return new ArtifactId(namespace.getNamespace(), artifactName, id.get("version").getAsString());
  }

  private DatasetId deserializeDatasetId(JsonObject id) {
    NamespaceId namespace = deserializeNamespace(id);
    String instanceId = id.get("dataset").getAsString();
    return new DatasetId(namespace.getNamespace(), instanceId);
  }

  private StreamId deserializeStreamId(JsonObject id) {
    NamespaceId namespace = deserializeNamespace(id);
    String streamName = id.get("stream").getAsString();
    return new StreamId(namespace.getNamespace(), streamName);
  }

  private StreamViewId deserializeViewId(JsonObject id) {
    StreamId streamId = deserializeStreamId(id);
    String view = id.get("view").getAsString();
    return new StreamViewId(streamId.getNamespace(), streamId.getStream(), view);
  }

  @Override
  public JsonElement serialize(NamespacedEntityId src, Type typeOfSrc, JsonSerializationContext context) {
    return context.serialize(src);
  }
}
