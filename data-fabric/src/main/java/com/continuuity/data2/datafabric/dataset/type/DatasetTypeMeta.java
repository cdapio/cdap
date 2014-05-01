package com.continuuity.data2.datafabric.dataset.type;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;

import java.util.List;

/**
 * Dataset type meta data
 */
public class DatasetTypeMeta {
  private final String name;
  private final List<DatasetModuleMeta> modules;

  /**
   * Creates instance of {@link DatasetTypeMeta}
   * @param name name of the dataset type
   * @param modules list of modules required to load this type in the same order as they must be loaded and initialized
   *                with the last one being the module that announces this type
   */
  public DatasetTypeMeta(String name, List<DatasetModuleMeta> modules) {
    this.name = name;
    this.modules = modules;
  }

  /**
   * @return name of this dataset type
   */
  public String getName() {
    return name;
  }

  /**
   * @return list of modules required to load this type in the same order as they must be loaded and initialized
   *         with the last one being the module that announces this type
   */
  public List<DatasetModuleMeta> getModules() {
    return modules;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("name", name)
      .add("modules", Joiner.on(",").skipNulls().join(modules))
      .toString();
  }
}
