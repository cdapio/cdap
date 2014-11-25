/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2;

import javax.annotation.Nullable;

/**
 * Performs namespacing for data set names.
 */
public interface DatasetNamespace {
  /**
   * @param name name of the dataset
   * @return namespaced name of the dataset
   */
  String namespace(String name);

  /**
   * @param name namespaced name of the dataset
   * @return original name of the dataset or null if name is not within this namespace
   */
  @Nullable
  String fromNamespaced(String name);

  /**
   * @param name namespaced name of the dataset
   * @return true if the dataset belongs to this namespace
   */
  boolean contains(String name);
}
