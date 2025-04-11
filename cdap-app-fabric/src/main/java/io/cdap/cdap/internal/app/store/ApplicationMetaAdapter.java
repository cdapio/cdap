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

package io.cdap.cdap.internal.app.store;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.store.adapters.PluginAdapter;
import io.cdap.cdap.internal.app.store.adapters.SerializationContext;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.StructuredTable;
import java.lang.reflect.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationMetaAdapter implements JsonSerializer<ApplicationMeta>,
    JsonDeserializer<ApplicationMeta> {

  private static final Logger LOG = LoggerFactory.getLogger(AppMetadataStore.class);
  private final SerializationContext context;

  public ApplicationMetaAdapter(SerializationContext context) {
    this.context = context;
  }

  @Override
  public JsonElement serialize(ApplicationMeta src, Type typeOfSrc,
      JsonSerializationContext context) {
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("id", src.getId());

    JsonObject specJson = context.serialize(src.getSpec()).getAsJsonObject();

    jsonObject.add("spec", specJson);
    LOG.debug("Post removal {}", specJson);
    return jsonObject;
  }

  @Override
  public ApplicationMeta deserialize(JsonElement json, Type typeOfT,
      JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObject = json.getAsJsonObject();
    String id = jsonObject.get("id").getAsString();
    JsonObject specJson = jsonObject.getAsJsonObject("spec");
    if (specJson != null) {
      ArtifactId artifactId = context.deserialize(specJson.get("artifactId"), ArtifactId.class);
      String parentName = artifactId.getName();
      String parentNamespace = artifactId.getScope().equals(ArtifactScope.SYSTEM) ?
          NamespaceId.SYSTEM.getNamespace() : this.context.getNamespace();
      this.context.setParentName(parentName);
      this.context.setParentNamespace(parentNamespace);
    }
    ApplicationSpecification spec = context.deserialize(specJson,
        ApplicationSpecification.class);

    return new ApplicationMeta(id, spec, null, null);
  }

  public static class PluginClassAdapter implements JsonSerializer<PluginClass> {

    @Override
    public JsonElement serialize(PluginClass src, Type typeOfSrc,
        JsonSerializationContext context) {
      JsonObject jsonObject = new JsonObject();
      jsonObject.addProperty("type", src.getType());
      jsonObject.addProperty("name", src.getName());
      // Exclude plugins here
      return jsonObject;
    }

  }


  // Utility method to create Gson with this adapter
  public static Gson createGson(StructuredTable pluginDataTable, String namespace) {
    SerializationContext context = new SerializationContext(namespace, pluginDataTable);
    GsonBuilder gson = new GsonBuilder()
        .registerTypeAdapter(ApplicationMeta.class, new ApplicationMetaAdapter(context))
        .registerTypeAdapter(PluginClass.class, new PluginClassAdapter())
        .registerTypeAdapter(Plugin.class, new PluginAdapter(context));
    gson = ApplicationSpecificationAdapter.addTypeAdapters(gson);
    return gson.create();
  }

}
