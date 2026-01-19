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
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.store.StoreDefinition.ArtifactStore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;

/**
 * Provides context for deserializing an {@code ApplicationSpecification}, primarily for resolving
 * and loading {@link PluginClass} instances. It holds the current namespace, caches loaded plugins,
 * and allows an outer deserializer to set parent artifact details (name and namespace) used for
 * plugin resolution.
 */
public class AppSpecDeserializationContext {

  private static final Gson GSON = new Gson();
  private ArtifactId rootArtifact;
  private String namespace;
  private String appName;
  private final StructuredTable pluginDataTable;
  private final StructuredTable universalPluginDataTable;
  private Set<String> missingPlugins;
  private final LoadingCache<PluginKey, PluginClass> pluginCache;

  /**
   * Constructs a context for deserializing an Application Specification.
   *
   * @param namespace                The current application's namespace.
   * @param pluginDataTable          Table for loading plugin data.
   * @param universalPluginDataTable Table for loading universal plugins.
   */
  public AppSpecDeserializationContext(String namespace, StructuredTable pluginDataTable,
      StructuredTable universalPluginDataTable) {
    this.namespace = namespace;
    this.pluginDataTable = pluginDataTable;
    this.universalPluginDataTable = universalPluginDataTable;
    this.missingPlugins = new HashSet<>();
    this.pluginCache = CacheBuilder.newBuilder().maximumSize(10)
        .build(new CacheLoader<PluginKey, PluginClass>() {
          @Override
          @Nonnull
          public PluginClass load(@Nonnull PluginKey pluginKey) throws PluginNotExistsException {
            return loadPlugin(pluginKey);
          }
        });
  }

  private PluginClass loadPlugin(PluginKey pluginKey) throws PluginNotExistsException {
    PluginClass pluginClass = null;
    Optional<StructuredRow> row = fetchFromPluginDataTable(pluginKey);
    if (!row.isPresent()) {
      row = fetchFromUniversalPluginDataTable(pluginKey);
      if (!row.isPresent()) {
        throw new PluginNotExistsException(
            new io.cdap.cdap.proto.id.ArtifactId(pluginKey.getArtifactNamespace(),
                pluginKey.getArtifactName(), pluginKey.getArtifactVersion()),
            pluginKey.getPluginType(), pluginKey.getPluginName());
      }
    }
    String rawPluginData = row.get().getString(ArtifactStore.PLUGIN_DATA_FIELD);
    if (rawPluginData != null) {
      JsonObject pluginDataObject = new JsonParser().parse(rawPluginData).getAsJsonObject();
      if (pluginDataObject.has("pluginClass") && pluginDataObject.get("pluginClass")
          .isJsonObject()) {
        pluginClass = GSON.fromJson(pluginDataObject.getAsJsonObject("pluginClass"),
            PluginClass.class);
      }
    }
    if (pluginClass == null) {
      throw new RuntimeException("Failed to deserialize PluginClass from data for key: " + pluginKey
          + ". Raw data might be missing or malformed.");
    }
    return pluginClass;
  }

  private Optional<StructuredRow> fetchFromPluginDataTable(PluginKey pluginKey) {
    if (pluginKey.getParentName() == null || pluginKey.getParentName().isEmpty()) {
      // For such scenarios, we need to fetch plugin data from universal plugin data table.
      return Optional.empty();
    }
    Collection<Field<?>> fields = new ArrayList<>();
    fields.add(
        Fields.stringField(ArtifactStore.PARENT_NAMESPACE_FIELD, pluginKey.getParentNamespace()));
    fields.add(Fields.stringField(ArtifactStore.PARENT_NAME_FIELD, pluginKey.getParentName()));
    fields.add(Fields.stringField(ArtifactStore.PLUGIN_TYPE_FIELD, pluginKey.getPluginType()));
    fields.add(Fields.stringField(ArtifactStore.PLUGIN_NAME_FIELD, pluginKey.getPluginName()));
    fields.add(Fields.stringField(ArtifactStore.ARTIFACT_NAMESPACE_FIELD,
        pluginKey.getArtifactNamespace()));
    fields.add(Fields.stringField(ArtifactStore.ARTIFACT_NAME_FIELD, pluginKey.getArtifactName()));
    fields.add(
        Fields.stringField(ArtifactStore.ARTIFACT_VER_FIELD, pluginKey.getArtifactVersion()));
    try {
      return pluginDataTable.read(fields);
    } catch (IOException e) {
      throw new RuntimeException("Failed to read from plugin data table for key: " + pluginKey, e);
    }
  }

  private Optional<StructuredRow> fetchFromUniversalPluginDataTable(PluginKey pluginKey) {
    Collection<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.stringField(ArtifactStore.NAMESPACE_FIELD, namespace));
    fields.add(Fields.stringField(ArtifactStore.PLUGIN_TYPE_FIELD, pluginKey.getPluginType()));
    fields.add(Fields.stringField(ArtifactStore.PLUGIN_NAME_FIELD, pluginKey.getPluginName()));
    fields.add(Fields.stringField(ArtifactStore.ARTIFACT_NAMESPACE_FIELD,
        pluginKey.getArtifactNamespace()));
    fields.add(Fields.stringField(ArtifactStore.ARTIFACT_NAME_FIELD, pluginKey.getArtifactName()));
    fields.add(
        Fields.stringField(ArtifactStore.ARTIFACT_VER_FIELD, pluginKey.getArtifactVersion()));
    try {
      return universalPluginDataTable.read(fields);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to read from universal plugin data table for key: " + pluginKey, e);
    }
  }

  /**
   * Gets the primary namespace for the current deserialization.
   *
   * @return The namespace string.
   */
  public String getNamespace() {
    return namespace;
  }

  /**
   * Sets the primary namespace for the current deserialization.
   */
  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  /**
   * Gets the root artifact, set externally.
   */
  public ArtifactId getRootArtifact() {
    return rootArtifact;
  }

  /**
   * Sets the root artifact for plugin resolution context.
   */
  public void setRootArtifact(ArtifactId rootArtifact) {
    this.rootArtifact = rootArtifact;
  }

  /**
   * Appends a plugin that could not be resolved during deserialization.
   */
  public void appendMissingPlugin(String pluginInfo) {
    if (pluginInfo != null) {
      this.missingPlugins.add(pluginInfo);
    }
  }

  /**
   * Returns a read-only view of the plugins that were missing during the deserialization process.
   */
  public Set<String> getMissingPlugins() {
    return Collections.unmodifiableSet(this.missingPlugins);
  }

  /**
   * Sets the application name.
   */
  public void setAppName(String appName) {
    this.appName = appName;
  }

  /**
   * Gets the application name, set externally.
   */
  public String getAppName() {
    return appName;
  }

  /**
   * Retrieves the {@link PluginClass} for the given key, using an internal cache.
   *
   * @param pluginKey Key identifying the plugin.
   * @return The resolved {@link PluginClass}.
   * @throws RuntimeException if the plugin cannot be loaded.
   */
  public PluginClass getPlugin(PluginKey pluginKey) throws PluginNotExistsException {
    try {
      return this.pluginCache.get(pluginKey);
    } catch (ExecutionException | UncheckedExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof PluginNotExistsException) {
        // cache.get() method wraps the PluginNotExistsException with Execution exception.
        // Hence, we need to explicitly re-throw the PluginNotExistsException here.
        throw (PluginNotExistsException) cause;
      }
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new RuntimeException("Failed to load plugin class for " + pluginKey, cause);
    }
  }
}
