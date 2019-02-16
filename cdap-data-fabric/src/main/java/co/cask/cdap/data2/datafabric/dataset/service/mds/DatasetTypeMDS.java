/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset.service.mds;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.id.DatasetModuleId;
import co.cask.cdap.proto.id.DatasetTypeId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Dataset types and modules metadata store
 */
public class DatasetTypeMDS extends MetadataStoreDataset {
  /**
   * Prefix for rows containing module info.
   * NOTE: we store in same table list of modules, with keys being <MODULES_PREFIX><module_name> and
   *       types to modules mapping with keys being <TYPE_TO_MODULE_PREFIX><type_name>
   */
  public static final String MODULES_PREFIX = "m_";

  /**
   * Prefix for rows containing type -> module mapping
   * see {@link #MODULES_PREFIX} for more info.
   */
  private static final String TYPE_TO_MODULE_PREFIX = "t_";

  private static final Gson BACKWARD_COMPAT_GSON = new GsonBuilder()
    .registerTypeAdapter(DatasetModuleId.class, new BackwardCompatDatasetModuleIdDeserializer()).create();

  public DatasetTypeMDS(DatasetSpecification spec, @EmbeddedDataset("") Table table) {
    super(table);
  }

  /**
   * Retrieves a module from the given namespace
   *
   * @param datasetModuleId the {@link DatasetModuleId} for the module to retrieve
   * @return {@link DatasetModuleMeta} for the module if found in the specified namespace, null otherwise
   */
  @Nullable
  public DatasetModuleMeta getModule(DatasetModuleId datasetModuleId) {
    return get(getModuleKey(datasetModuleId.getNamespace(), datasetModuleId.getEntityName()), DatasetModuleMeta.class);
  }

  /**
   * Tries to find a module in the specified namespace first. If it fails, tries to find it in the system namespace
   *
   * @param datasetModuleId {@link DatasetModuleId} for the module to retrieve
   * @return {@link DatasetModuleMeta} for the module if found either in the specified namespace or in the system
   * namespace, null otherwise
   */
  @Nullable
  public DatasetModuleMeta getModuleWithFallback(DatasetModuleId datasetModuleId) {
    // Try to find module in the specified namespace first
    DatasetModuleMeta moduleMeta = getModule(datasetModuleId);
    // if not found, try to load it from system namespace
    if (moduleMeta == null) {
      moduleMeta = getModule(NamespaceId.SYSTEM.datasetModule(datasetModuleId.getEntityName()));
    }
    return moduleMeta;
  }

  @Nullable
  public DatasetModuleMeta getModuleByType(DatasetTypeId datasetTypeId) {
    DatasetModuleId datasetModuleId = get(getTypeKey(datasetTypeId.getNamespace(), datasetTypeId.getEntityName()),
                                           DatasetModuleId.class);
    if (datasetModuleId == null) {
      return null;
    }
    // TODO: Slightly strange. Maybe change signature to accept NamespaceId separately from typeName
    return getModule(datasetModuleId);
  }

  public DatasetTypeMeta getType(DatasetTypeId datasetTypeId) {
    DatasetModuleMeta moduleName = getModuleByType(datasetTypeId);
    if (moduleName == null) {
      return null;
    }
    return getTypeMeta(datasetTypeId.getParent(), datasetTypeId.getEntityName(), moduleName);
  }

  public Collection<DatasetModuleMeta> getModules(NamespaceId namespaceId) {
    return list(getModuleKey(namespaceId.getEntityName()), DatasetModuleMeta.class);
  }

  public Collection<DatasetTypeMeta> getTypes(NamespaceId namespaceId) {
    List<DatasetTypeMeta> types = Lists.newArrayList();
    for (Map.Entry<MDSKey, DatasetModuleId> entry : getTypesMapping(namespaceId).entrySet()) {
      MDSKey.Splitter splitter = entry.getKey().split();
      // first part is TYPE_TO_MODULE_PREFIX
      splitter.skipString();
      // second part is namespace
      splitter.skipString();
      // third part is the type name, which is what we expect
      String typeName = splitter.getString();
      types.add(getTypeMeta(namespaceId, typeName, entry.getValue()));
    }
    return types;
  }

  public void writeModule(NamespaceId namespaceId, DatasetModuleMeta moduleMeta) {
    DatasetModuleId datasetModuleId = namespaceId.datasetModule(moduleMeta.getName());
    DatasetModuleMeta existing = getModule(datasetModuleId);
    write(getModuleKey(namespaceId.getEntityName(), moduleMeta.getName()), moduleMeta);
    for (String type : moduleMeta.getTypes()) {
      writeTypeToModuleMapping(namespaceId.datasetType(type), datasetModuleId);
    }
    if (existing != null) {
      Set<String> removed = new HashSet<>(existing.getTypes());
      removed.removeAll(moduleMeta.getTypes());
      for (String type : removed) {
        deleteAll(getTypeKey(datasetModuleId.getNamespace(), type));
      }
    }
  }

  public void deleteModule(DatasetModuleId datasetModuleId) {
    DatasetModuleMeta module = getModule(datasetModuleId);
    if (module == null) {
      // that's fine: module is not there
      return;
    }

    deleteAll(getModuleKey(datasetModuleId.getNamespace(), datasetModuleId.getEntityName()));

    for (String type : module.getTypes()) {
      deleteAll(getTypeKey(datasetModuleId.getNamespace(), type));
    }
  }

  public void deleteModules(NamespaceId namespaceId) {
    Collection<DatasetModuleMeta> modules = getModules(namespaceId);
    for (DatasetModuleMeta module : modules) {
      deleteModule(namespaceId.datasetModule(module.getName()));
    }
  }

  private DatasetTypeMeta getTypeMeta(NamespaceId namespaceId, String typeName, DatasetModuleId datasetModuleId) {
    DatasetModuleMeta moduleMeta = getModule(datasetModuleId);
    return getTypeMeta(namespaceId, typeName, moduleMeta);
  }

  private DatasetTypeMeta getTypeMeta(NamespaceId namespaceId, String typeName, DatasetModuleMeta moduleMeta) {
    List<DatasetModuleMeta> modulesToLoad = Lists.newArrayList();
    // adding first all modules we depend on, then myself
    for (String usedModule : moduleMeta.getUsesModules()) {
      // Try to find module in the specified namespace first, then the system namespace
      DatasetModuleMeta usedModuleMeta = getModuleWithFallback(namespaceId.datasetModule(usedModule));
      // Module could not be found in either user or system namespace, bail out
      Preconditions.checkState(usedModuleMeta != null,
                               String.format("Unable to find metadata about module %s that module %s uses.",
                                             usedModule, moduleMeta.getName()));
      modulesToLoad.add(usedModuleMeta);
    }
    modulesToLoad.add(moduleMeta);

    return new DatasetTypeMeta(typeName, modulesToLoad);
  }

  // type -> moduleName
  private Map<MDSKey, DatasetModuleId> getTypesMapping(NamespaceId namespaceId) {
    return listKV(getTypeKey(namespaceId.getEntityName()), DatasetModuleId.class);
  }

  private void writeTypeToModuleMapping(DatasetTypeId datasetTypeId, DatasetModuleId datasetModuleId) {
    write(getTypeKey(datasetTypeId.getNamespace(), datasetTypeId.getEntityName()), datasetModuleId);
  }

  private MDSKey getModuleKey(String namespace) {
    return getModuleKey(namespace, null);
  }

  private MDSKey getModuleKey(String namespace, @Nullable String moduleName) {
    return getKey(MODULES_PREFIX, namespace, moduleName);
  }

  private MDSKey getTypeKey(String namespace) {
    return getTypeKey(namespace, null);
  }

  private MDSKey getTypeKey(String namespace, @Nullable String typeName) {
    return getKey(TYPE_TO_MODULE_PREFIX, namespace, typeName);
  }

  private MDSKey getKey(String type, String namespace, @Nullable String name) {
    MDSKey.Builder builder = new MDSKey.Builder().add(type).add(namespace);
    if (name != null) {
      builder.add(name);
    }
    return builder.build();
  }

  @Override
  protected <T> T deserialize(@Nullable MDSKey key, byte[] serialized, Type typeOfT) {
    return BACKWARD_COMPAT_GSON.fromJson(Bytes.toString(serialized), typeOfT);
  }

  @VisibleForTesting
  <T> T deserializeProxy(byte[] serialized, Type typeOfT) {
    // We never use the key, hence just pass in null.
    return deserialize(null, serialized, typeOfT);
  }

  /**
   * A JsonDeserializer that can decode the pre-4.0 Id.DatasetModule as a 4.0+ DatasetModuleId.
   */
  private static class BackwardCompatDatasetModuleIdDeserializer implements JsonDeserializer<DatasetModuleId> {

    private static final Gson GSON = new Gson();

    @Override
    public DatasetModuleId deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
      throws JsonParseException {
      if (!(json instanceof JsonObject)) {
        throw new JsonParseException("Expected JsonObject but found " + json.getClass().getSimpleName());
      }
      JsonObject object = (JsonObject) json;
      // 4.0: {“module":"metricsTable-hbase","namespace":"system","entity":"DATASET_MODULE”}
      if (object.has("entity")) {
        return GSON.fromJson(json, DatasetModuleId.class);
      }
      // 3.5: {"namespace":{"id":"system"},"moduleId":"metricsTable-hbase"}
      try {
        return new DatasetModuleId(object.getAsJsonObject("namespace").get("id").getAsString(),
                                   object.get("moduleId").getAsString());
      } catch (Exception e) {
        throw new JsonParseException("Failed to deserialize as 3.5 Id.DatasetModule: " + json, e);
      }
    }
  }
}
