/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.proto.codec;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.proto.id.ServiceId;
import io.cdap.cdap.proto.id.WorkerId;
import io.cdap.cdap.proto.id.WorkflowId;

import java.lang.reflect.Type;

/**
 * Codec for {@link io.cdap.cdap.proto.id.NamespacedEntityId}.
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
      case "artifact":
        return deserializeArtifactId(jsonObj);
      default:
        throw new UnsupportedOperationException(
          String.format("Unsupported object of entity %s found. Deserialization of only %s, %s, %s, %s, %s, %s, " +
                          "%s, %s is supported.",
                        entity,
                        ApplicationId.class.getSimpleName(),
                        ProgramId.class.getSimpleName(),
                        ServiceId.class.getSimpleName(),
                        ScheduleId.class.getSimpleName(),
                        WorkerId.class.getSimpleName(),
                        WorkflowId.class.getSimpleName(),
                        DatasetId.class.getSimpleName(),
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

  @Override
  public JsonElement serialize(NamespacedEntityId src, Type typeOfSrc, JsonSerializationContext context) {
    return context.serialize(src);
  }
}
