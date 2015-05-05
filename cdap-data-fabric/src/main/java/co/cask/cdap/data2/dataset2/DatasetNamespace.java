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

package co.cask.cdap.data2.dataset2;

import co.cask.cdap.proto.Id;

import javax.annotation.Nullable;

/**
 * Performs namespacing for datasets.
 */
public interface DatasetNamespace {

  /**
   * Namespaces (applies the namespace as a prefix) to the specified dataset instance name
   * Used for dataset instances in the system namespace
   * Calls #namespace(Id.DatasetInstance.from(Constants.SYSTEM_NAMESPACE, datasetInstanceName)
   * @see #namespace(Id.DatasetInstance)
   *
   * @param datasetInstanceName name of the dataset instance to be namespaced
   * @return namespaced {@link Id.DatasetInstance} of the dataset
   */
  Id.DatasetInstance namespace(String datasetInstanceName);

  /**
   * Namespaces (applies the namespace as a prefix) to the specified {@link Id.DatasetInstance}
   * {@code
   * String namespacedInstanceName = datasetInstanceId.getNamespace() + "." + datasetInstanceId.getId();
   * return Id.DatasetInstance.from(datasetInstanceId.getNamespace(), namespacedInstanceName)
   * }
   * e.g. If Id.DatasetInstance(default, purchases) is passed, it will return
   * Id.DatasetInstance(default, cdap.default.purchases)
   *
   * @param datasetInstanceId {@link Id.DatasetInstance} for the dataset instance to be namespaced
   * @return namespaced {@link Id.DatasetInstance} of the dataset
   */
  Id.DatasetInstance namespace(Id.DatasetInstance datasetInstanceId);

  /**
   * Namespaces (applies the namespace as a prefix) to the specified suffix
   * Used for applying namespace to part table names
   *
   * @param namespaceId the {@link Id.Namespace} to namespace the suffix with
   * @param suffix the suffix to namespace
   * @return String containing the suffix prefixed with the specified namespace
   */
  String namespace(Id.Namespace namespaceId, String suffix);

  /**
   * Returns a new {@link Id.DatasetInstance} with the namespaceId prefix removed from the specified instance name
   * e.g. if cdap.myspace.myinstance is passed, this will return Id.DatasetInstance(myspace, myinstance)
   * @see #fromNamespaced(Id.DatasetInstance)
   *
   * @param namespaced namespaced name of the dataset
   * @return original {@link Id.DatasetInstance} of the dataset or null if name is not within this namespace
   */
  Id.DatasetInstance fromNamespaced(String namespaced);

  /**
   * Returns a new {@link Id.DatasetInstance} with the namespaceId prefix removed from the instance name in the
   * specified {@link Id.DatasetInstance}
   * e.g. If Id.DatasetInstance(default, cdap.default.purchases) is passed, this will return
   * Id.DatasetInstance(default, purchases)
   *
   * @param datasetInstanceId namespaced {@link Id.DatasetInstance} of the dataset
   * @return original {@link Id.DatasetInstance} of the dataset or null if name is not within this namespace
   */
  @Nullable
  Id.DatasetInstance fromNamespaced(Id.DatasetInstance datasetInstanceId);

  /**
   * Checks if the specified namespace contains the specified dataset instance name
   *
   * @param name namespaced name of the dataset
   * @return true if the dataset belongs to this namespace
   */
  boolean contains(String name, String namespaceId);
}
