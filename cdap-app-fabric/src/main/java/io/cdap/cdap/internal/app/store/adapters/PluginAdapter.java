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
import com.google.gson.JsonParser;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.internal.guava.reflect.TypeParameter;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.store.StoreDefinition.ArtifactStore;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PluginAdapter implements JsonSerializer<Plugin>, JsonDeserializer<Plugin> {

  private static final Logger LOG = LoggerFactory.getLogger(PluginAdapter.class);

  public PluginAdapter(StructuredTable pluginDataTable) {
    this.pluginDataTable = pluginDataTable;
  }

  private final StructuredTable pluginDataTable;

  @Override
  public JsonElement serialize(Plugin src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObject = new JsonObject();
    jsonObject.add("artifactId", context.serialize(src.getArtifactId(), ArtifactId.class));
    jsonObject.add("pluginClass", context.serialize(src.getPluginClass(), PluginClass.class));
    // Exclude plugins here
    return jsonObject;
  }

  @Override
  public Plugin deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
      throws JsonParseException {
    JsonObject jsonObject = json.getAsJsonObject();
    List<ArtifactId> parents = deserializeList(jsonObject.get("handlers"), context,
        ArtifactId.class);
    ArtifactId artifactId = context.deserialize(jsonObject.get("artifactId"), ArtifactId.class);
    PluginClass pluginClass = context.deserialize(jsonObject.get("pluginClass"), PluginClass.class);
    // Fill up plugin class data
    if (pluginClass.getDescription().isEmpty()) {
      Collection<Field<?>> fields = new ArrayList<>();
      fields.add(Fields.stringField(ArtifactStore.PARENT_NAMESPACE_FIELD, "system"));
      fields.add(Fields.stringField(ArtifactStore.PARENT_NAME_FIELD, "cdap-data-pipeline"));
      fields.add(Fields.stringField(ArtifactStore.PLUGIN_TYPE_FIELD, pluginClass.getType()));
      fields.add(Fields.stringField(ArtifactStore.PLUGIN_NAME_FIELD, pluginClass.getName()));
      fields.add(
          Fields.stringField(ArtifactStore.ARTIFACT_NAMESPACE_FIELD, artifactId.getScope().name()));
      fields.add(Fields.stringField(ArtifactStore.ARTIFACT_NAME_FIELD, artifactId.getName()));
      fields.add(Fields.stringField(ArtifactStore.ARTIFACT_VER_FIELD,
          artifactId.getVersion().getVersion()));
      Optional<StructuredRow> row;
      try {
        row = pluginDataTable.read(fields);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (!row.isPresent()) {
        throw new RuntimeException("Plugin not present");
      }
      String rawPluginData = row.get().getString(ArtifactStore.PLUGIN_DATA_FIELD);
      if (rawPluginData != null) {
        JsonObject pluginDataObject = new JsonParser().parse(rawPluginData).getAsJsonObject();
        if (pluginDataObject.has("pluginClass") && pluginDataObject.get("pluginClass")
            .isJsonObject()) {
          pluginClass = context.deserialize(pluginDataObject.get("pluginClass"), PluginClass.class);
        }
      }
    }

    PluginProperties properties = context.deserialize(jsonObject.get("properties"),
        PluginProperties.class);

    Plugin plugin = new Plugin(parents, artifactId, pluginClass, properties);
    LOG.info("Plugin : {}", plugin);
    return plugin;
  }

  protected final <V> List<V> deserializeList(JsonElement json, JsonDeserializationContext context,
      Class<V> valueType) {
    Type type = new TypeToken<List<V>>() {
    }.where(new TypeParameter<V>() {
    }, valueType).getType();
    List<V> list = context.deserialize(json, type);
    return list == null ? Collections.<V>emptyList() : list;
  }
}
