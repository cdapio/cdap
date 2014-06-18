package com.continuuity.data2.dataset2.module.lib;

import com.continuuity.api.dataset.Dataset;
import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.data2.dataset2.SingleTypeModule;

/**
 * Utility class for dealing with {@link DatasetModule}s
 */
public final class DatasetModules {

  private DatasetModules() {}

  /**
   * Creates {@link DatasetModule} given a class of {@link DatasetModule} type or {@link Dataset}. In latter case uses
   * {@link SingleTypeModule} to construct a {@link DatasetModule}.
   * @param clazz class to be used
   * @return {@link DatasetModule} instance
   */
  public static DatasetModule getDatasetModule(Class clazz)
    throws ClassNotFoundException, InstantiationException, IllegalAccessException {

    DatasetModule module;
    if (DatasetModule.class.isAssignableFrom(clazz)) {
      module = (DatasetModule) clazz.newInstance();
    } else if (Dataset.class.isAssignableFrom(clazz)) {
      module = new SingleTypeModule(clazz);
    } else {
      String msg = String.format(
        "Cannot use class %s to instantiate dataset module: it must be of type DatasetModule or Dataset",
        clazz.getName());
      throw new IllegalArgumentException(msg);
    }
    return module;
  }

}
