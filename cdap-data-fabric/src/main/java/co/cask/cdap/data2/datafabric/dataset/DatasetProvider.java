/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.cdap.data2.datafabric.dataset;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.proto.Id;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Provides {@link Dataset} instances.
 */
public interface DatasetProvider {

  /**
   * Gets a dataset.
   *
   * @param instance the dataset ID
   * @param classLoader the classloader to use
   * @param arguments the runtime arguments for the dataset
   * @param <T> the type of dataset
   * @return the dataset
   */
  <T extends Dataset> T get(
    Id.DatasetInstance instance,
    @Nullable ClassLoader classLoader,
    @Nullable Map<String, String> arguments) throws Exception;

  /**
   * Gets a dataset, and creates it first if it doesn't yet exist.
   *
   * @param instance the dataset ID
   * @param type the type of dataset
   * @param creationProps the creation properties
   * @param classLoader the classloader to use
   * @param arguments the runtime arguments for the dataset
   * @param <T> the type of dataset
   * @return the dataset
   */
  <T extends Dataset> T getOrCreate(
    Id.DatasetInstance instance,
    String type,
    DatasetProperties creationProps,
    @Nullable ClassLoader classLoader,
    @Nullable Map<String, String> arguments) throws Exception;
}
