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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.store.StoreDefinition.ArtifactStore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class SerializationContext {

  private String parentName;
  private String parentNamespace;
  private final String namespace;
  private final StructuredTable pluginDataTable;
  private final LoadingCache<PluginKey, PluginClass> pluginCache;

  public SerializationContext(String namespace, StructuredTable pluginDataTable) {
    this.namespace = namespace;
    this.pluginDataTable = pluginDataTable;
    this.pluginCache = CacheBuilder.newBuilder().maximumSize(10)
        .build(new CacheLoader<PluginKey, PluginClass>() {
          @Override
          public PluginClass load(PluginKey pluginKey) {
            return loadPlugin(pluginKey);
          }
        });
  }

  private PluginClass loadPlugin(PluginKey pluginKey) {
    PluginClass pluginClass = null;
    Collection<Field<?>> fields = new ArrayList<>();
    fields.add(
        Fields.stringField(ArtifactStore.PARENT_NAMESPACE_FIELD, pluginKey.getParentNamespace()));
    fields.add(Fields.stringField(ArtifactStore.PARENT_NAME_FIELD, pluginKey.getParentName()));
    fields.add(Fields.stringField(ArtifactStore.PLUGIN_TYPE_FIELD, pluginKey.getPluginType()));
    fields.add(Fields.stringField(ArtifactStore.PLUGIN_NAME_FIELD, pluginKey.getPluginName()));
    fields.add(
        Fields.stringField(ArtifactStore.ARTIFACT_NAMESPACE_FIELD,
            pluginKey.getArtifactNamespace()));
    fields.add(Fields.stringField(ArtifactStore.ARTIFACT_NAME_FIELD, pluginKey.getArtifactName()));
    fields.add(Fields.stringField(ArtifactStore.ARTIFACT_VER_FIELD,
        pluginKey.getArtifactVersion()));
    Optional<StructuredRow> row;
    try {
      row = pluginDataTable.read(fields);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (!row.isPresent()) {
      throw new RuntimeException("Plugin not present : " + pluginKey);
    }
    String rawPluginData = row.get().getString(ArtifactStore.PLUGIN_DATA_FIELD);
    if (rawPluginData != null) {
      JsonObject pluginDataObject = new JsonParser().parse(rawPluginData).getAsJsonObject();
      if (pluginDataObject.has("pluginClass") && pluginDataObject.get("pluginClass")
          .isJsonObject()) {
        Gson gson = new Gson();
        pluginClass = gson.fromJson(pluginDataObject.getAsJsonObject("pluginClass"),
            PluginClass.class);
      }
    }
    return pluginClass;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getParentName() {
    return parentName;
  }

  public String getParentNamespace() {
    return parentNamespace;
  }

  public void setParentName(String parentName) {
    this.parentName = parentName;
  }

  public void setParentNamespace(String parentNamespace) {
    this.parentNamespace = parentNamespace;
  }

  public PluginClass getPlugin(PluginKey pluginKey) {
    try {
      return this.pluginCache.get(pluginKey);
    } catch (ExecutionException | UncheckedExecutionException e) {
      throw new RuntimeException("Failed to load plugin class for " + pluginKey, e.getCause());
    }
  }
}
