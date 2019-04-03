/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.id.DatasetModuleId;
import co.cask.cdap.proto.id.DatasetTypeId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.spi.data.StructuredRow;
import co.cask.cdap.spi.data.StructuredTable;
import co.cask.cdap.spi.data.StructuredTableContext;
import co.cask.cdap.spi.data.TableNotFoundException;
import co.cask.cdap.spi.data.table.field.Field;
import co.cask.cdap.spi.data.table.field.Fields;
import co.cask.cdap.spi.data.table.field.Range;
import co.cask.cdap.store.StoreDefinition;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.Gson;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Dataset types and modules metadata store
 */
public class DatasetTypeTable {

  private static final Gson GSON = new Gson();

  private final StructuredTableContext structuredTableContext;

  private StructuredTable typeTable;
  private StructuredTable moduleTable;

  private DatasetTypeTable(StructuredTableContext structuredTableContext) {
    this.structuredTableContext = structuredTableContext;
  }

  public static DatasetTypeTable create(StructuredTableContext context) {
    return new DatasetTypeTable(context);
  }

  private StructuredTable getTypeTable() {
    if (typeTable == null) {
      try {
        typeTable = structuredTableContext.getTable(StoreDefinition.DatasetTypeStore.DATASET_TYPES);
      } catch (TableNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
    return typeTable;
  }

  private StructuredTable getModuleTable() {
    if (moduleTable == null) {
      try {
        moduleTable = structuredTableContext.getTable(StoreDefinition.DatasetTypeStore.MODULE_TYPES);
      } catch (TableNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
    return moduleTable;
  }


  /**
   * Retrieves a module from the given namespace
   *
   * @param datasetModuleId the {@link DatasetModuleId} for the module to retrieve
   * @return {@link DatasetModuleMeta} for the module if found in the specified namespace, null otherwise
   */
  @Nullable
  public DatasetModuleMeta getModule(DatasetModuleId datasetModuleId) throws IOException {
    return get(getModuleKey(datasetModuleId.getNamespace(), datasetModuleId.getEntityName()),
               getModuleTable(), DatasetModuleMeta.class);
  }

  /**
   * Tries to find a module in the specified namespace first. If it fails, tries to find it in the system namespace
   *
   * @param datasetModuleId {@link DatasetModuleId} for the module to retrieve
   * @return {@link DatasetModuleMeta} for the module if found either in the specified namespace or in the system
   * namespace, null otherwise
   */
  @Nullable
  public DatasetModuleMeta getModuleWithFallback(DatasetModuleId datasetModuleId) throws IOException {
    // Try to find module in the specified namespace first
    DatasetModuleMeta moduleMeta = getModule(datasetModuleId);
    // if not found, try to load it from system namespace
    if (moduleMeta == null) {
      moduleMeta = getModule(NamespaceId.SYSTEM.datasetModule(datasetModuleId.getEntityName()));
    }
    return moduleMeta;
  }

  @Nullable
  public DatasetModuleMeta getModuleByType(DatasetTypeId datasetTypeId) throws IOException {
    DatasetModuleId datasetModuleId =
      get(getTypeKey(
        datasetTypeId.getNamespace(), datasetTypeId.getEntityName()), getTypeTable(), DatasetModuleId.class);

    if (datasetModuleId == null) {
      return null;
    }
    return getModule(datasetModuleId);
  }

  public DatasetTypeMeta getType(DatasetTypeId datasetTypeId) throws IOException {
    DatasetModuleMeta moduleName = getModuleByType(datasetTypeId);
    if (moduleName == null) {
      return null;
    }
    return getTypeMeta(datasetTypeId.getParent(), datasetTypeId.getEntityName(), moduleName);
  }

  public Collection<DatasetModuleMeta> getModules(NamespaceId namespaceId) throws IOException {
    return list(getModulePrefix(namespaceId.getEntityName()), getModuleTable(), DatasetModuleMeta.class);
  }

  public Collection<DatasetTypeMeta> getTypes(NamespaceId namespaceId) throws IOException {
    List<DatasetTypeMeta> types = Lists.newArrayList();
    try (CloseableIterator<StructuredRow> iterator =
      getTypeTable().scan(Range.singleton(getModulePrefix(namespaceId.getEntityName())), Integer.MAX_VALUE)) {
      while (iterator.hasNext()) {
        StructuredRow row = iterator.next();
        String typeName = row.getString(StoreDefinition.DatasetTypeStore.TYPE_NAME_FIELD);
        DatasetModuleId moduleId =
          GSON.fromJson(row.getString(StoreDefinition.DatasetTypeStore.DATASET_METADATA_FIELD),
                        DatasetModuleId.class);
        types.add(getTypeMeta(namespaceId, typeName, moduleId));
      }
    }
    return types;
  }

  public void writeModule(NamespaceId namespaceId, DatasetModuleMeta moduleMeta) throws IOException {
    DatasetModuleId datasetModuleId = namespaceId.datasetModule(moduleMeta.getName());
    DatasetModuleMeta existing = getModule(datasetModuleId);
    List<Field<?>> fields = getModuleKey(namespaceId.getEntityName(), moduleMeta.getName());
    fields.add(Fields.stringField(StoreDefinition.DatasetTypeStore.DATASET_METADATA_FIELD, GSON.toJson(moduleMeta)));
    getModuleTable().upsert(fields);
    for (String type : moduleMeta.getTypes()) {
      writeTypeToModuleMapping(namespaceId.datasetType(type), datasetModuleId);
    }
    if (existing != null) {
      Set<String> removed = new HashSet<>(existing.getTypes());
      removed.removeAll(moduleMeta.getTypes());
      for (String type : removed) {
        getTypeTable().deleteAll(Range.singleton(getTypeKey(datasetModuleId.getNamespace(), type)));
      }
    }
  }

  public void deleteModule(DatasetModuleId datasetModuleId) throws IOException {
    DatasetModuleMeta module = getModule(datasetModuleId);
    if (module == null) {
      // that's fine: module is not there
      return;
    }

    getModuleTable().deleteAll(
      Range.singleton(getModuleKey(datasetModuleId.getNamespace(), datasetModuleId.getEntityName())));

    for (String type : module.getTypes()) {
      getTypeTable().deleteAll(Range.singleton(getTypeKey(datasetModuleId.getNamespace(), type)));
    }
  }

  public void deleteModules(NamespaceId namespaceId) throws IOException {
    Collection<DatasetModuleMeta> modules = getModules(namespaceId);
    for (DatasetModuleMeta module : modules) {
      deleteModule(namespaceId.datasetModule(module.getName()));
    }
  }

  private DatasetTypeMeta getTypeMeta(NamespaceId namespaceId, String typeName, DatasetModuleId datasetModuleId)
    throws IOException {
    DatasetModuleMeta moduleMeta = getModule(datasetModuleId);
    return getTypeMeta(namespaceId, typeName, moduleMeta);
  }

  private DatasetTypeMeta getTypeMeta(NamespaceId namespaceId, String typeName, DatasetModuleMeta moduleMeta)
    throws IOException {
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

  private void writeTypeToModuleMapping(DatasetTypeId datasetTypeId, DatasetModuleId datasetModuleId)
    throws IOException {
    List<Field<?>> fields = getTypeKey(datasetTypeId.getNamespace(), datasetTypeId.getEntityName());
    fields.add(Fields.stringField(
      StoreDefinition.DatasetTypeStore.DATASET_METADATA_FIELD, GSON.toJson(datasetModuleId)));
    getTypeTable().upsert(fields);
  }

  private List<Field<?>> getModulePrefix(String namespace) {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.stringField(StoreDefinition.DatasetTypeStore.NAMESPACE_FIELD, namespace));
    return fields;
  }

  private List<Field<?>> getModuleKey(String namespace, String moduleName) {
    List<Field<?>> fields = getModulePrefix(namespace);
    fields.add(Fields.stringField(StoreDefinition.DatasetTypeStore.MODULE_NAME_FIELD, moduleName));
    return fields;
  }

  private List<Field<?>> getTypePrefix(String namespace) {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.stringField(StoreDefinition.DatasetTypeStore.NAMESPACE_FIELD, namespace));
    return fields;
  }

  private List<Field<?>> getTypeKey(String namespace, String typeName) {
    List<Field<?>> fields = getTypePrefix(namespace);
    fields.add(Fields.stringField(StoreDefinition.DatasetTypeStore.TYPE_NAME_FIELD, typeName));
    return fields;
  }

  @Nullable
  private <T> T get(List<Field<?>> keys, StructuredTable table, Type typeofT) throws IOException {
    Optional<StructuredRow> row = table.read(keys);
    if (!row.isPresent()) {
      return null;
    }
    return GSON.fromJson(row.get().getString(StoreDefinition.DatasetTypeStore.DATASET_METADATA_FIELD), typeofT);
  }

  private <T> Collection<T> list(List<Field<?>> prefix, StructuredTable table, Type typeofT)
    throws IOException {
    List<T> result = new ArrayList<>();
    try (CloseableIterator<StructuredRow> iterator = table.scan(Range.singleton(prefix), Integer.MAX_VALUE)) {
      while (iterator.hasNext()) {
        result.add(
          GSON.fromJson(iterator.next().getString(StoreDefinition.DatasetTypeStore.DATASET_METADATA_FIELD), typeofT));
      }
    }
    return result;
  }
}
