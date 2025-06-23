/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.provision.adapters;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.internal.app.store.adapters.AppSpecDeserializationContext;
import io.cdap.cdap.internal.app.store.adapters.AppSpecDeserializationContextHolder;
import io.cdap.cdap.proto.id.ProgramId;
import java.lang.reflect.Type;

/**
 * Custom Gson deserializer for {@link ProgramDescriptor}. This deserializer is responsible for
 * reconstructing a {@link ProgramDescriptor} from its JSON representation, particularly by setting
 * the root artifact in the {@link AppSpecDeserializationContext} to enable correct plugin
 * resolution during the deserialization of the nested {@link ApplicationSpecification}.
 */
public class ProgramDescriptorDeserializer implements JsonDeserializer<ProgramDescriptor> {

  @Override
  public ProgramDescriptor deserialize(JsonElement json, Type typeOfT,
      JsonDeserializationContext context) throws JsonParseException {
    AppSpecDeserializationContext appSpecDeserializationContext = AppSpecDeserializationContextHolder.getContext();
    JsonObject jsonObject = json.getAsJsonObject();

    JsonElement programIdElement = jsonObject.get("programId");
    if (programIdElement == null || programIdElement.isJsonNull()) {
      throw new JsonParseException("ApplicationMeta 'id' field is missing or null");
    }
    ProgramId programId = context.deserialize(programIdElement, ProgramId.class);
    appSpecDeserializationContext.setNamespace(programId.getNamespaceId().getNamespace());

    JsonObject specJson = jsonObject.getAsJsonObject("appSpec");
    if (specJson == null || specJson.isJsonNull()) {
      throw new JsonParseException(
          "ApplicationMeta 'spec' field is missing or null for id: " + programId.getApplication());
    }
    // Set parent context before fully deserializing the AppSpec,
    // so plugins can be retrieved from DB using this parent information.
    JsonElement artifactIdJson = specJson.get("artifactId");
    if (artifactIdJson != null && !artifactIdJson.isJsonNull()) {
      ArtifactId artifactId = context.deserialize(artifactIdJson, ArtifactId.class);
      if (artifactId != null) {
        appSpecDeserializationContext.setRootArtifact(artifactId);
      } else {
        throw new JsonParseException("ArtifactId in ApplicationSpecification was null for app: "
            + programId.getApplication());
      }
    }
    ApplicationSpecification spec = context.deserialize(specJson, ApplicationSpecification.class);
    return new ProgramDescriptor(programId, spec);
  }
}
