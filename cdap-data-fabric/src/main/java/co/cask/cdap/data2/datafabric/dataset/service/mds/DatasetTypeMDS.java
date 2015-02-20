/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.Id;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;
import java.util.Map;
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
  private static final String MODULES_PREFIX = "m_";

  /**
   * Prefix for rows containing type -> module mapping
   * see {@link #MODULES_PREFIX} for more info.
   */
  private static final String TYPE_TO_MODULE_PREFIX = "t_";

  public DatasetTypeMDS(DatasetSpecification spec, @EmbeddedDataset("") Table table) {
    super(table);
  }

  /**
   * Retrieves a module from the given namespace
   *
   * @param datasetModuleId the {@link Id.DatasetModule} for the module to retrieve
   * @return {@link DatasetModuleMeta} for the module if found in the specified namespace, null otherwise
   */
  @Nullable
  public DatasetModuleMeta getModule(Id.DatasetModule datasetModuleId) {
    return get(getModuleKey(datasetModuleId.getNamespaceId(), datasetModuleId.getId()), DatasetModuleMeta.class);
  }

  /**
   * Tries to find a module in the specified namespace first. If it fails, tries to find it in the system namespace
   *
   * @param datasetModuleId {@link Id.DatasetModule} for the module to retrieve
   * @return {@link DatasetModuleMeta} for the module if found either in the specified namespace or in the system
   * namespace, null otherwise
   */
  @Nullable
  public DatasetModuleMeta getModuleWithFallback(Id.DatasetModule datasetModuleId) {
    // Try to find module in the specified namespace first
    DatasetModuleMeta moduleMeta = getModule(datasetModuleId);
    // if not found, try to load it from system namespace
    if (moduleMeta == null) {
      moduleMeta = getModule(Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE_ID, datasetModuleId.getId()));
    }
    return moduleMeta;
  }

  @Nullable
  public DatasetModuleMeta getModuleByType(Id.DatasetType datasetTypeId) {
    Id.DatasetModule datasetModuleId = get(getTypeKey(datasetTypeId.getNamespaceId(), datasetTypeId.getTypeName()),
                                           Id.DatasetModule.class);
    if (datasetModuleId == null) {
      return null;
    }
    // TODO: Slightly strange. Maybe change signature to accept Id.Namespace separately from typeName
    return getModule(datasetModuleId);
  }

  public DatasetTypeMeta getType(Id.DatasetType datasetTypeId) {
    DatasetModuleMeta moduleName = getModuleByType(datasetTypeId);
    if (moduleName == null) {
      return null;
    }
    return getTypeMeta(datasetTypeId.getNamespace(), datasetTypeId.getTypeName(), moduleName);
  }

  public Collection<DatasetModuleMeta> getModules(Id.Namespace namespaceId) {
    return list(getModuleKey(namespaceId.getId()), DatasetModuleMeta.class);
  }

  public Collection<DatasetTypeMeta> getTypes(Id.Namespace namespaceId) {
    List<DatasetTypeMeta> types = Lists.newArrayList();
    for (Map.Entry<MDSKey, Id.DatasetModule> entry : getTypesMapping(namespaceId).entrySet()) {
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

  public void writeModule(Id.Namespace namespaceId, DatasetModuleMeta moduleMeta) {
    Id.DatasetModule datasetModuleId = Id.DatasetModule.from(namespaceId, moduleMeta.getName());
    write(getModuleKey(namespaceId.getId(), moduleMeta.getName()), moduleMeta);

    for (String type : moduleMeta.getTypes()) {
      writeTypeToModuleMapping(Id.DatasetType.from(namespaceId, type), datasetModuleId);
    }
  }

  public void deleteModule(Id.DatasetModule datasetModuleId) {
    DatasetModuleMeta module = getModule(datasetModuleId);
    if (module == null) {
      // that's fine: module is not there
      return;
    }

    deleteAll(getModuleKey(datasetModuleId.getNamespaceId(), datasetModuleId.getId()));

    for (String type : module.getTypes()) {
      deleteAll(getTypeKey(datasetModuleId.getNamespaceId(), type));
    }
  }

  public void deleteModules(Id.Namespace namespaceId) {
    Collection<DatasetModuleMeta> modules = getModules(namespaceId);
    for (DatasetModuleMeta module : modules) {
      deleteModule(Id.DatasetModule.from(namespaceId, module.getName()));
    }
  }

  private DatasetTypeMeta getTypeMeta(Id.Namespace namespaceId, String typeName, Id.DatasetModule datasetModuleId) {
    DatasetModuleMeta moduleMeta = getModule(datasetModuleId);
    return getTypeMeta(namespaceId, typeName, moduleMeta);
  }

  private DatasetTypeMeta getTypeMeta(Id.Namespace namespaceId, String typeName, DatasetModuleMeta moduleMeta) {
    List<DatasetModuleMeta> modulesToLoad = Lists.newArrayList();
    // adding first all modules we depend on, then myself
    for (String usedModule : moduleMeta.getUsesModules()) {
      // Try to find module in the specified namespace first, then the system namespace
      DatasetModuleMeta usedModuleMeta = getModuleWithFallback(Id.DatasetModule.from(namespaceId, usedModule));
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
  private Map<MDSKey, Id.DatasetModule> getTypesMapping(Id.Namespace namespaceId) {
    return listKV(getTypeKey(namespaceId.getId()), Id.DatasetModule.class);
  }

  private void writeTypeToModuleMapping(Id.DatasetType datasetTypeId, Id.DatasetModule datasetModuleId) {
    write(getTypeKey(datasetTypeId.getNamespaceId(), datasetTypeId.getTypeName()), datasetModuleId);
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
}
