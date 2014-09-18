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

package co.cask.cdap.api.dataset.module;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.dataset.DatasetDefinition;

/**
 * Registry of dataset definitions and other components in a Datasets System.
 *
 * The implementation of this interface doesn't have to be thread-safe.
 */
@Beta
public interface DatasetDefinitionRegistry {
  /**
   * Adds {@link co.cask.cdap.api.dataset.DatasetDefinition} to the registry.
   *
   * After it was added it is available thru {@link #get(String)} method.
   *
   * @param def definition to add
   * @throws IllegalArgumentException if registry already contains dataset type of the same name as given definition
   */
  void add(DatasetDefinition def);

  /**
   * Gets {@link DatasetDefinition} previously added to the registry.
   * @param datasetTypeName dataset type name, should be same as
   *                        {@link co.cask.cdap.api.dataset.DatasetDefinition#getName()}
   * @param <T> type of the returned {@link DatasetDefinition}
   * @return instance of {@link DatasetDefinition}
   * @throws IllegalArgumentException if registry does not contain dataset type of a given name
   */
  <T extends DatasetDefinition> T get(String datasetTypeName);

  /**
   * @param datasetTypeName name of the dataset type
   * @return true if registry contains dataset type of the given name, false otherwise
   */
  boolean hasType(String datasetTypeName);
}
