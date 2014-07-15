/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.api.dataset.module;

import com.continuuity.api.annotation.Beta;
import com.continuuity.api.dataset.DatasetDefinition;

/**
 * Registry of dataset definitions and other components in a Datasets System.
 *
 * The implementation of this interface doesn't have to be thread-safe.
 */
@Beta
public interface DatasetDefinitionRegistry {
  /**
   * Adds {@link com.continuuity.api.dataset.DatasetDefinition} to the registry.
   *
   * After it was added it is available thru {@link #get(String)} method.
   *
   * @param def definition to add
   */
  void add(DatasetDefinition def);

  /**
   * Gets {@link DatasetDefinition} previously added to the registry.
   * @param datasetTypeName dataset type name, should be same as
   *                        {@link com.continuuity.api.dataset.DatasetDefinition#getName()}
   * @param <T> type of the returned {@link DatasetDefinition}
   * @return instance of {@link DatasetDefinition}
   */
  <T extends DatasetDefinition> T get(String datasetTypeName);
}
