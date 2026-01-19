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

package io.cdap.cdap.internal.app.store.adapters;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.internal.app.store.ApplicationMeta;
import java.lang.reflect.Type;

/**
 * Helper class to encode/decode {@link ApplicationMetaCodec} to/from json.
 */
public class ApplicationMetaCodec implements JsonSerializer<ApplicationMeta>,
    JsonDeserializer<ApplicationMeta> {

  /**
   * Serializes an {@link ApplicationMeta} object to its JSON representation. Includes the
   * application ID and its specification.
   */
  @Override
  public JsonElement serialize(ApplicationMeta src, Type typeOfSrc,
      JsonSerializationContext context) {
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("id", src.getId());

    JsonElement specJson = context.serialize(src.getSpec());
    jsonObject.add("spec", specJson);

    return jsonObject;
  }

  /**
   * Deserializes a JSON element into an {@link ApplicationMeta} object. It extracts the
   * application's {@link ArtifactId} from its specification JSON to set the parent name and
   * namespace in the shared {@link AppSpecDeserializationContext}. This enables correct resolution
   * of plugins within the application's scope.
   *
   * @throws JsonParseException If JSON is malformed or required fields are missing.
   */
  @Override
  public ApplicationMeta deserialize(JsonElement json, Type typeOfT,
      JsonDeserializationContext context) throws JsonParseException {
    AppSpecDeserializationContext appSpecDeserializationContext = AppSpecDeserializationContextHolder.getContext();
    JsonObject jsonObject = json.getAsJsonObject();

    JsonElement idElement = jsonObject.get("id");
    if (idElement == null || idElement.isJsonNull()) {
      throw new JsonParseException("ApplicationMeta 'id' field is missing or null");
    }
    String id = idElement.getAsString();

    JsonObject specJson = jsonObject.getAsJsonObject("spec");
    if (specJson == null || specJson.isJsonNull()) {
      throw new JsonParseException("ApplicationMeta 'spec' field is missing or null for id: " + id);
    }

    // Set parent context before fully deserializing the AppSpec,
    // so plugins can be retrieved from DB using this parent information.
    JsonElement artifactIdJson = specJson.get("artifactId");
    if (artifactIdJson != null && !artifactIdJson.isJsonNull()) {
      ArtifactId artifactId = context.deserialize(artifactIdJson, ArtifactId.class);
      if (artifactId != null) {
        appSpecDeserializationContext.setRootArtifact(artifactId);
      } else {
        throw new JsonParseException(
            "ArtifactId in ApplicationSpecification was null for app: " + id);
      }
    }

    JsonElement appNameJson = specJson.get("name");
    appSpecDeserializationContext.setAppName(appNameJson == null ? null : appNameJson.getAsString());

    ApplicationSpecification spec = context.deserialize(specJson, ApplicationSpecification.class);
    return new ApplicationMeta(id, spec, null, null);
  }
}
