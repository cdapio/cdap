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

package co.cask.cdap.data2.dataset2.module.lib;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.data2.dataset2.SingleTypeModule;

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

    if (DatasetModule.class.isAssignableFrom(clazz)) {
      return (DatasetModule) clazz.newInstance();
    }
    if (Dataset.class.isAssignableFrom(clazz)) {
      return new SingleTypeModule(clazz);
    }

    String msg = String.format(
      "Cannot use class %s to instantiate dataset module; it must be of type DatasetModule or Dataset",
      clazz.getName());
    throw new IllegalArgumentException(msg);
  }

  /**
   * Gets class of the dataset module to be used in {@link #getDatasetModule(Class)};
   *
   * We support easier APIs for custom datasets: user can implement dataset and make it available for others to use
   * by only implementing Dataset. Without requiring implementing datasets module, definition and other classes.
   * In this case we wrap that Dataset implementation with SingleTypeModule. But since we don't have a way to serde
   * dataset modules, if we pass only SingleTypeModule.class the Dataset implementation info will be lost. Hence, as
   * a workaround we put Dataset implementation class in MDS (on DatasetService) and wrapping it with SingleTypeModule
   * when we need to instantiate module.
   */
  public static Class getDatasetModuleClass(DatasetModule module) {
    if (module instanceof SingleTypeModule) {
      return ((SingleTypeModule) module).getDataSetClass();
    }
    return module.getClass();
  }
}
