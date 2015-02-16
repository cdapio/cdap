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
   * @param datasetInstanceName name of the dataset instance to be namespaced
   * @return namespaced {@link Id.DatasetInstance} of the dataset
   */
  Id.DatasetInstance namespace(String datasetInstanceName);

  /**
   * @param datasetInstanceId {@link Id.DatasetInstance} for the dataset instance to be namespaced
   * @return namespaced {@link Id.DatasetInstance} of the dataset
   */
  Id.DatasetInstance namespace(Id.DatasetInstance datasetInstanceId);

  /**
   * @param namespaceId the {@link Id.Namespace} to namespace the suffix with
   * @param suffix the suffix to namespace
   * @return String containing the suffix prefixed with the specified namespace
   */
  String namespace(Id.Namespace namespaceId, String suffix);

  /**
   * @param datasetInstanceName namespaced name of the dataset
   * @return original {@link Id.DatasetInstance} of the dataset or null if name is not within this namespace
   */
  @Nullable
  Id.DatasetInstance fromNamespaced(String datasetInstanceName);

  /**
   * @param datasetInstanceId namespaced {@link Id.DatasetInstance} of the dataset
   * @return original {@link Id.DatasetInstance} of the dataset or null if name is not within this namespace
   */
  @Nullable
  Id.DatasetInstance fromNamespaced(Id.DatasetInstance datasetInstanceId);

  /**
   * @param name namespaced name of the dataset
   * @return true if the dataset belongs to this namespace
   */
  boolean contains(String name, String namespaceId);
}
