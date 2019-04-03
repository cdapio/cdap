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

import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;

import javax.annotation.Nullable;

/**
 * Performs namespacing for datasets.
 */
public interface DatasetNamespace {

  /**
   * Namespaces (applies the namespace as a prefix) to the specified dataset instance name
   * Used for dataset instances in the system namespace
   * Calls #namespace(DatasetId.from(NamespaceId.SYSTEM, datasetInstanceName)
   * @see #namespace(DatasetId)
   *
   * @param datasetInstanceName name of the dataset instance to be namespaced
   * @return namespaced {@link DatasetId} of the dataset
   */
  DatasetId namespace(String datasetInstanceName);

  /**
   * Namespaces (applies the namespace as a prefix) to the specified {@link DatasetId}
   * {@code
   * String namespacedInstanceName = datasetInstanceId.getNamespace() + "." + datasetInstanceId.getId();
   * return DatasetId.from(datasetInstanceId.getNamespace(), namespacedInstanceName)
   * }
   * e.g. If DatasetId(default, purchases) is passed, it will return
   * DatasetId(default, cdap.default.purchases)
   *
   * @param datasetInstanceId {@link DatasetId} for the dataset instance to be namespaced
   * @return namespaced {@link DatasetId} of the dataset
   */
  DatasetId namespace(DatasetId datasetInstanceId);

  /**
   * Namespaces (applies the namespace as a prefix) to the specified suffix
   * Used for applying namespace to part table names
   *
   * @param namespaceId the {@link NamespaceId} to namespace the suffix with
   * @param suffix the suffix to namespace
   * @return String containing the suffix prefixed with the specified namespace
   */
  String namespace(NamespaceId namespaceId, String suffix);

  /**
   * Returns a new {@link DatasetId} with the namespaceId prefix removed from the specified instance name
   * e.g. if cdap.myspace.myinstance is passed, this will return DatasetId(myspace, myinstance)
   * @see #fromNamespaced(DatasetId)
   *
   * @param namespaced namespaced name of the dataset
   * @return original {@link DatasetId} of the dataset or null if name is not within this namespace
   */
  DatasetId fromNamespaced(String namespaced);

  /**
   * Returns a new {@link DatasetId} with the namespaceId prefix removed from the instance name in the
   * specified {@link DatasetId}
   * e.g. If DatasetId(default, cdap.default.purchases) is passed, this will return
   * DatasetId(default, purchases)
   *
   * @param datasetInstanceId namespaced {@link DatasetId} of the dataset
   * @return original {@link DatasetId} of the dataset or null if name is not within this namespace
   */
  @Nullable
  DatasetId fromNamespaced(DatasetId datasetInstanceId);

  /**
   * Checks if the specified namespace contains the specified dataset instance name
   *
   * @param name namespaced name of the dataset
   * @return true if the dataset belongs to this namespace
   */
  boolean contains(String name, String namespaceId);
}
