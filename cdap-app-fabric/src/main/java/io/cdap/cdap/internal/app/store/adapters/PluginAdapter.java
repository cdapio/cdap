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
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.internal.guava.reflect.TypeParameter;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
import io.cdap.cdap.proto.id.NamespaceId;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PluginAdapter implements JsonSerializer<Plugin>, JsonDeserializer<Plugin> {

  private static final Logger LOG = LoggerFactory.getLogger(PluginAdapter.class);
  private final SerializationContext serializationContext;

  public PluginAdapter(SerializationContext context) {
    this.serializationContext = context;
  }

  @Override
  public JsonElement serialize(Plugin src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObject = new JsonObject();
    jsonObject.add("parents",
        serializeList(src.getParents(), context, ArtifactId.class));
    jsonObject.add("artifactId", context.serialize(src.getArtifactId(), ArtifactId.class));
    jsonObject.add("pluginClass", context.serialize(src.getPluginClass(), PluginClass.class));
    // Exclude plugins here
    return jsonObject;
  }

  @Override
  public Plugin deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
      throws JsonParseException {
    JsonObject jsonObject = json.getAsJsonObject();
    List<ArtifactId> parents = deserializeList(jsonObject.get("parents"), context,
        ArtifactId.class);
    String pluginParent = "";
    String pluginParentNamespace = "";
    if (!parents.isEmpty()) {
      pluginParent = parents.get(0).getName();
      pluginParentNamespace = parents.get(0).getScope().equals(ArtifactScope.SYSTEM) ?
          NamespaceId.SYSTEM.getNamespace() : serializationContext.getNamespace();
    }
    ArtifactId artifactId = context.deserialize(jsonObject.get("artifactId"), ArtifactId.class);
    PluginClass pluginClass = context.deserialize(jsonObject.get("pluginClass"), PluginClass.class);
    // Fill up plugin class data
    if (pluginClass.getDescription().isEmpty()) {
      String artifactNamespace = artifactId.getScope().equals(ArtifactScope.SYSTEM) ?
          NamespaceId.SYSTEM.getNamespace() : serializationContext.getNamespace();
      String parentName = (pluginClass.getType().equals("directive") && !pluginParent.equals("")) ?
          pluginParent : serializationContext.getParentName();
      String parentNamespace =
          (pluginClass.getType().equals("directive") && !pluginParent.equals("")) ?
              pluginParentNamespace : serializationContext.getParentNamespace();
      PluginKey pluginKey = new PluginKey(parentName, parentNamespace,
          artifactId.getName(), artifactNamespace, artifactId.getVersion().getVersion(),
          pluginClass.getType(), pluginClass.getName());
      pluginClass = serializationContext.getPlugin(pluginKey);
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

  protected final <V> JsonElement serializeList(List<V> list, JsonSerializationContext context,
      Class<V> valueType) {
    Type type = new TypeToken<List<V>>() {
    }.where(new TypeParameter<V>() {
    }, valueType).getType();
    return context.serialize(list, type);
  }
}
