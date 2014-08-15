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

package co.cask.cdap.proto;

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
