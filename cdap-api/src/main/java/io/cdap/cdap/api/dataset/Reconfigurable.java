/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.api.dataset;

/**
 * Interface implemented by DatasetDefinitions that have a way to reconfigure the dataset properties.
 * It is optional for a dataset type to implement this interface; if not implemented, the dataset system
 * will assume that no compatibility check is required and call
 * {@link DatasetDefinition#configure(String, DatasetProperties)} instead to obtain the new dataset specification.
 */
public interface Reconfigurable {

  /**
   * Validates the new properties, including a compatibility check with the existing spec, and returns a
   * new dataset specification for the dataset instance.
   *
   * @param newProperties the updated dataset properties, to be validated by this method
   * @param currentSpec the current specification of the dataset
   * @return a new dataset specification, created from the new properties
   * @throws IncompatibleUpdateException if the new properties are not compatible with the existing dataset
   */
  DatasetSpecification reconfigure(String instanceName,
                                   DatasetProperties newProperties,
                                   DatasetSpecification currentSpec)
    throws IncompatibleUpdateException;
}
