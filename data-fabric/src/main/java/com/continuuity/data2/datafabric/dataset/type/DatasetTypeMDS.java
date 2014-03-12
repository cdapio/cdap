package com.continuuity.data2.datafabric.dataset.type;

import com.continuuity.api.common.Bytes;
import com.continuuity.data2.datafabric.dataset.AbstractObjectsStore;
import com.continuuity.internal.data.dataset.lib.table.OrderedTable;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Dataset types and modules metadata store
 */
final class DatasetTypeMDS extends AbstractObjectsStore {
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

  public DatasetTypeMDS(OrderedTable table) {
    super(table);
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
