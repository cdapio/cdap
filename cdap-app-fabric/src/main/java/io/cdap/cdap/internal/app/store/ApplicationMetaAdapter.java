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
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.store.adapters.PluginAdapter;
import io.cdap.cdap.spi.data.StructuredTable;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationMetaAdapter implements JsonSerializer<ApplicationMeta>,
    JsonDeserializer<ApplicationMeta> {

  private static final Logger LOG = LoggerFactory.getLogger(AppMetadataStore.class);

  public ApplicationMetaAdapter(StructuredTable pluginDataTable) {
    this.pluginDataTable = pluginDataTable;
    this.results = new HashMap<>();
  }

  private final StructuredTable pluginDataTable;
  private Map<PluginKey, String> results;

  @Override
  public JsonElement serialize(ApplicationMeta src, Type typeOfSrc,
      JsonSerializationContext context) {
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("id", src.getId());

    JsonObject specJson = context.serialize(src.getSpec()).getAsJsonObject();

    // Custom serialization logic for AbstractProgramSpecification subclasses
//    customizeSpecJson(specJson, context);

    jsonObject.add("spec", specJson);
    LOG.info("Post removal {}", specJson);
    return jsonObject;
  }

  private void customizeSpecJson(JsonObject specJson, JsonSerializationContext context) {
    LOG.info("Removing plugin fields from here now :{}", specJson.toString());
    // Iterate through the fields of the ApplicationSpecification JSON
    for (Map.Entry<String, JsonElement> entry : specJson.entrySet()) {
      if (entry.getValue().isJsonObject()) {
        JsonObject programJson = entry.getValue().getAsJsonObject();
        // Check if the current program is an instance of AbstractProgramSpecification
        if (programJson.has("className") && programJson.has("name") && programJson.has(
            "description")) {
          programJson.remove("plugins"); // Remove the plugins field
        }
      } else if (entry.getValue().isJsonObject() && entry.getKey().equals("plugins")) {
        JsonObject pluginsJson = entry.getValue().getAsJsonObject();
        for (Map.Entry<String, JsonElement> pluginEntry : pluginsJson.entrySet()) {
          JsonObject pluginJson = pluginEntry.getValue().getAsJsonObject();
          JsonObject pluginClassJson = pluginJson.getAsJsonObject("pluginClass");
          pluginJson.remove("properties");
          pluginJson.remove("parents");
          pluginJson.remove("pluginClass");
          pluginJson.add("pluginClass", pluginClassJson);
        }
      }
    }
  }

  @Override
  public ApplicationMeta deserialize(JsonElement json, Type typeOfT,
      JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObject = json.getAsJsonObject();
    String id = jsonObject.get("id").getAsString();
//    ArtifactId artifactId = context.deserialize(jsonObject.get("artifactId"), ArtifactId.class);
//    String parentName = artifactId.getName();
//    String parentNamespace = artifactId.getScope().name();
//
//    Set<PluginKey> plugins = findPluginKeys(jsonObject);
//    results = new HashMap<>();
//    for (PluginKey plugin : plugins) {
//      Collection<Field<?>> fields = new ArrayList<>();
//      try {
//        fields.add(Fields.stringField(ArtifactStore.PARENT_NAME_FIELD, parentName));
//        fields.add(Fields.stringField(ArtifactStore.PARENT_NAMESPACE_FIELD, parentNamespace));
//        fields.add(Fields.stringField(ArtifactStore.ARTIFACT_NAME_FIELD, plugin.getArtifactName()));
//        fields.add(Fields.stringField(ArtifactStore.ARTIFACT_NAMESPACE_FIELD, plugin.getArtifactNamespace()));
//        fields.add(Fields.stringField(ArtifactStore.ARTIFACT_VER_FIELD, plugin.getArtifactVersion()));
//        fields.add(Fields.stringField(ArtifactStore.PLUGIN_NAME_FIELD, plugin.getPluginName()));
//        fields.add(Fields.stringField(ArtifactStore.PLUGIN_TYPE_FIELD, plugin.getPluginType()));
//        Optional<StructuredRow> row = pluginDataTable.read(fields);
//        if (!row.isPresent()) {
//          throw new RuntimeException("Plugin not present");
//        }
//        String rawPluginData = row.get().getString(ArtifactStore.PLUGIN_DATA_FIELD);
//        results.put(plugin, rawPluginData);
//      } catch (IOException e) {
//        throw new RuntimeException(e);
//      }
//    }

    ApplicationSpecification spec = context.deserialize(jsonObject.get("spec"),
        ApplicationSpecification.class);

    return new ApplicationMeta(id, spec, null, null);
  }

  public Set<PluginKey> findPluginKeys(JsonObject jsonObject) {
    Set<PluginKey> pluginKeys = new HashSet<>();
    findPluginObjectsRecursive(jsonObject, pluginKeys);
    return pluginKeys;
  }

  public class PluginKey {

    private String parentName;

    public String getParentName() {
      return parentName;
    }

    public String getParentNamespace() {
      return parentNamespace;
    }

    public String getArtifactName() {
      return artifactName;
    }

    public String getArtifactNamespace() {
      return artifactNamespace;
    }

    public String getArtifactVersion() {
      return artifactVersion;
    }

    public String getPluginName() {
      return pluginName;
    }

    public String getPluginType() {
      return pluginType;
    }

    private String parentNamespace;
    private String artifactName;
    private String artifactNamespace;
    private String artifactVersion;
    private String pluginName;
    private String pluginType;

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof PluginKey)) {
        return false;
      }
      PluginKey pluginKey = (PluginKey) o;
      return Objects.equals(parentName, pluginKey.parentName) && Objects.equals(
          parentNamespace, pluginKey.parentNamespace) && Objects.equals(artifactName,
          pluginKey.artifactName) && Objects.equals(artifactNamespace,
          pluginKey.artifactNamespace) && Objects.equals(artifactVersion,
          pluginKey.artifactVersion) && Objects.equals(pluginName, pluginKey.pluginName)
          && Objects.equals(pluginType, pluginKey.pluginType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(parentName, parentNamespace, artifactName, artifactNamespace,
          artifactVersion,
          pluginName, pluginType);
    }

    public void setParentName(String parentName) {
      this.parentName = parentName;
    }

    public void setParentNamespace(String parentNamespace) {
      this.parentNamespace = parentNamespace;
    }

    public void setArtifactName(String artifactName) {
      this.artifactName = artifactName;
    }

    public void setArtifactNamespace(String artifactNamespace) {
      this.artifactNamespace = artifactNamespace;
    }

    public void setArtifactVersion(String artifactVersion) {
      this.artifactVersion = artifactVersion;
    }

    public void setPluginName(String pluginName) {
      this.pluginName = pluginName;
    }

    public void setPluginType(String pluginType) {
      this.pluginType = pluginType;
    }
  }

  private void findPluginObjectsRecursive(JsonObject jsonObject,
      Set<PluginKey> pluginKeys) {
    if (jsonObject.has("plugins") && jsonObject.get("plugins").isJsonObject()) {
      JsonObject pluginsObject = jsonObject.getAsJsonObject("plugins");
      for (Map.Entry<String, JsonElement> entry : pluginsObject.entrySet()) {
        if (entry.getValue().isJsonObject()) {
          JsonObject pluginObject = entry.getValue().getAsJsonObject();
          PluginKey pluginKey = new PluginKey();
          if (pluginsObject.has("artifactId") && jsonObject.get("artifactId").isJsonObject()) {
            pluginKey.setArtifactName("1");
            pluginKey.setArtifactNamespace("2");
            pluginKey.setArtifactVersion("3");
          }
          if (pluginsObject.has("pluginClass") && jsonObject.get("pluginClass").isJsonObject()) {
            pluginKey.setPluginName("4");
            pluginKey.setPluginType("5");
          }
          pluginKeys.add(pluginKey);
        }
      }
    }

    for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
      JsonElement element = entry.getValue();
      if (element.isJsonObject()) {
        findPluginObjectsRecursive(element.getAsJsonObject(), pluginKeys);
      } else if (element.isJsonArray()) {
        JsonArray jsonArray = element.getAsJsonArray();
        for (JsonElement arrayElement : jsonArray) {
          if (arrayElement.isJsonObject()) {
            findPluginObjectsRecursive(arrayElement.getAsJsonObject(), pluginKeys);
          }
        }
      }
    }
  }

  public static class PluginClassAdapter implements JsonSerializer<PluginClass> {

    @Override
    public JsonElement serialize(PluginClass src, Type typeOfSrc,
        JsonSerializationContext context) {
      JsonObject jsonObject = new JsonObject();
//      jsonObject.add("artifactId", context.serialize(src.getArtifactId(), Artifact.class));
      jsonObject.addProperty("type", src.getType());
      jsonObject.addProperty("name", src.getName());
      // Exclude plugins here
      return jsonObject;
    }

  }


  // Utility method to create Gson with this adapter
  public static Gson createGson(StructuredTable pluginDataTable) {
    GsonBuilder gson = new GsonBuilder()
        .registerTypeAdapter(ApplicationMeta.class, new ApplicationMetaAdapter(pluginDataTable))
        .registerTypeAdapter(PluginClass.class, new PluginClassAdapter())
        .registerTypeAdapter(Plugin.class, new PluginAdapter(pluginDataTable));
    gson = ApplicationSpecificationAdapter.addTypeAdapters(gson);
    return gson.create();
  }

}
