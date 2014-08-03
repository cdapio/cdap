/*
 * Copyright 2014 Cask, Inc.
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
import co.cask.cdap.api.dataset.table.OrderedTable;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Dataset types and modules metadata store
 */
public class DatasetTypeMDS extends AbstractObjectsStore {
  /**
   * Prefix for rows containing module info.
   * NOTE: we store in same table list of modules, with keys being <MODULES_PREFIX><module_name> and
   *       types to modules mapping with keys being <TYPE_TO_MODULE_PREFIX><type_name>
   */
  private static final byte[] MODULES_PREFIX = Bytes.toBytes("m_");

  /**
   * Prefix for rows containing type -> module mapping
   * see {@link #MODULES_PREFIX} for more info.
   */
  private static final byte[] TYPE_TO_MODULE_PREFIX = Bytes.toBytes("t_");

  public DatasetTypeMDS(DatasetSpecification spec, @EmbeddedDataset("") OrderedTable table) {
    super(spec, table);
  }

  @Nullable
  public DatasetModuleMeta getModule(String name) {
    return get(getModuleKey(name), DatasetModuleMeta.class);
  }

  @Nullable
  public DatasetModuleMeta getModuleByType(String typeName) {
    String moduleName = get(getTypeKey(typeName), String.class);
    if (moduleName == null) {
      return null;
    }
    return getModule(moduleName);
  }

  public DatasetTypeMeta getType(String typeName) {
    DatasetModuleMeta moduleName = getModuleByType(typeName);
    if (moduleName == null) {
      return null;
    }
    return getTypeMeta(typeName, moduleName);
  }

  public Collection<DatasetModuleMeta> getModules() {
    byte[] prefix = getModuleKey("");
    return scan(prefix, DatasetModuleMeta.class).values();
  }

  public Collection<DatasetTypeMeta> getTypes() {
    List<DatasetTypeMeta> types = Lists.newArrayList();
    for (Map.Entry<String, String> entry : getTypesMapping().entrySet()) {
      types.add(getTypeMeta(entry.getKey(), entry.getValue()));
    }
    return types;
  }

  public void write(DatasetModuleMeta moduleMeta) {
    put(getModuleKey(moduleMeta.getName()), moduleMeta);

    for (String type : moduleMeta.getTypes()) {
      write(type, moduleMeta.getName());
    }
  }

  public void deleteModule(String name) {
    DatasetModuleMeta module = getModule(name);
    if (module == null) {
      // that's fine: module is not there
      return;
    }

    delete(getModuleKey(module.getName()));

    for (String type : module.getTypes()) {
      delete(getTypeKey(type));
    }
  }

  private DatasetTypeMeta getTypeMeta(String typeName, String moduleName) {
    DatasetModuleMeta moduleMeta = getModule(moduleName);
    return getTypeMeta(typeName, moduleMeta);
  }

  private DatasetTypeMeta getTypeMeta(String typeName, DatasetModuleMeta moduleMeta) {
    List<DatasetModuleMeta> modulesToLoad = Lists.newArrayList();
    // adding first all modules we depend on, then myself
    for (String usedModule : moduleMeta.getUsesModules()) {
      modulesToLoad.add(getModule(usedModule));
    }
    modulesToLoad.add(moduleMeta);

    return new DatasetTypeMeta(typeName, modulesToLoad);
  }

  // type -> moduleName
  private Map<String, String> getTypesMapping() {
    byte[] prefix = getTypeKey("");
    return scan(prefix, String.class);
  }

  private void write(String typeName, String moduleName) {
    put(getTypeKey(typeName), moduleName);
  }

  private byte[] getModuleKey(String name) {
    return Bytes.add(MODULES_PREFIX, Bytes.toBytes(name));
  }

  private byte[] getTypeKey(String name) {
    return Bytes.add(TYPE_TO_MODULE_PREFIX, Bytes.toBytes(name));
  }
}
