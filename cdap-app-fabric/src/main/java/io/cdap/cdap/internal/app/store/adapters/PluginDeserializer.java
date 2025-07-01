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

import com.google.common.collect.Iterables;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import io.cdap.cdap.internal.guava.reflect.TypeParameter;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
import io.cdap.cdap.proto.id.NamespaceId;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;

/**
 * Gson deserializer for {@link Plugin}s. Uses {@link AppSpecDeserializationContext} to enrich
 * {@link PluginClass} data if initially minimal.
 */
public class PluginDeserializer implements JsonDeserializer<Plugin> {

  /**
   * Deserializes JSON to {@link Plugin}. If {@link PluginClass#getClassName()} is empty, attempts
   * to enrich it via {@link #resolvePluginClassData(List, ArtifactId, PluginClass)}.
   *
   * @param json    The JSON element to deserialize.
   * @param typeOfT The type of the object to deserialize to.
   * @param context The Gson deserialization context.
   * @return Deserialized {@link Plugin} object.
   * @throws JsonParseException If JSON is malformed.
   */
  @Override
  public Plugin deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
      throws JsonParseException {
    JsonObject jsonObject = json.getAsJsonObject();
    List<ArtifactId> parents = deserializeList(jsonObject.get("parents"), context,
        ArtifactId.class);
    ArtifactId artifactId = context.deserialize(jsonObject.get("artifactId"), ArtifactId.class);
    PluginClass pluginClass = context.deserialize(jsonObject.get("pluginClass"), PluginClass.class);

    // Populate missing PluginClass data which was removed during serialization.
    // This can be identified if the className field is empty.
    if (pluginClass != null && (pluginClass.getClassName() == null || pluginClass.getClassName()
        .isEmpty())) {
      pluginClass = resolvePluginClassData(parents, artifactId, pluginClass);
    }

    PluginProperties properties = context.deserialize(jsonObject.get("properties"),
        PluginProperties.class);

    return new Plugin(parents, artifactId, pluginClass, properties);
  }

  /**
   * Attempts to load {@link PluginClass} details from {@link AppSpecDeserializationContext}. Parent
   * context for lookup defaults to application context; for some plugins, it may be overridden by
   * artifact in the {@code parents} list if specific conditions are met. Returns the result of
   * {@code appSpecDeserializationContext.getPlugin(pluginKey)}.
   *
   * @param parents     Declared parent {@link ArtifactId}s from JSON.
   * @param artifactId  The plugin's own {@link ArtifactId}.
   * @param pluginClass Initial {@link PluginClass}, possibly incomplete.
   * @return {@link PluginClass} from context, or the outcome of the lookup.
   */
  private PluginClass resolvePluginClassData(List<ArtifactId> parents, ArtifactId artifactId,
      PluginClass pluginClass) {
    AppSpecDeserializationContext appSpecDeserializationContext = AppSpecDeserializationContextHolder.getContext();
    String artifactNamespace = resolveNamespaceByScope(artifactId.getScope(),
        appSpecDeserializationContext.getNamespace());

    Exception exception = null;
    ArtifactId rootArtifact = appSpecDeserializationContext.getRootArtifact();
    for (ArtifactId parentId : Iterables.concat(parents, Collections.singleton(rootArtifact))) {
      String parentName = parentId.getName();
      String parentNamespace = resolveNamespaceByScope(parentId.getScope(),
          appSpecDeserializationContext.getNamespace());
      PluginKey pluginKey = new PluginKey(parentName, parentNamespace, artifactId.getName(),
          artifactNamespace, artifactId.getVersion().getVersion(), pluginClass.getType(),
          pluginClass.getName());
      try {
        return appSpecDeserializationContext.getPlugin(pluginKey);
      } catch (PluginNotExistsException e) {
        exception = e;
      }
    }
    throw new RuntimeException(
        "No data found in plugin data table OR universal plugin data table for key: " + exception);
  }

  /**
   * Deserializes a JSON array into a {@link List} of {@code valueType}. Returns an empty list if
   * JSON is null, not an array, or if deserialization results in null.
   */
  private <V> List<V> deserializeList(JsonElement json, JsonDeserializationContext context,
      Class<V> valueType) {
    if (json == null || json.isJsonNull() || !json.isJsonArray()) {
      return Collections.emptyList();
    }
    Type type = new TypeToken<List<V>>() {
    }.where(new TypeParameter<V>() {
    }, valueType).getType();
    List<V> list = context.deserialize(json, type);
    return list == null ? Collections.emptyList() : list;
  }

  /**
   * Resolves namespace string from {@link ArtifactScope}. Uses system namespace for
   * {@link ArtifactScope#SYSTEM}, otherwise from context.
   *
   * @param scope The artifact scope.
   * @return Corresponding namespace string.
   */
  private String resolveNamespaceByScope(ArtifactScope scope, String namespace) {
    return ArtifactScope.SYSTEM.equals(scope) ? NamespaceId.SYSTEM.getNamespace() : namespace;
  }
}
