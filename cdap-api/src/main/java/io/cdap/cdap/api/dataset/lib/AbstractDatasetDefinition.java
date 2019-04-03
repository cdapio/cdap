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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.IncompatibleUpdateException;
import co.cask.cdap.api.dataset.Reconfigurable;

/**
 * Basic abstract implementation of {@link DatasetDefinition}.
 * @param <D> defines data operations that can be performed on this dataset instance
 * @param <A> defines administrative operations that can be performed on this dataset instance
 */
@Beta
public abstract class AbstractDatasetDefinition<D extends Dataset, A extends DatasetAdmin>
  implements DatasetDefinition<D, A> {

  private final String name;

  /**
   * Ctor that takes in name of this dataset type.
   * @param name this dataset type name
   */
  protected AbstractDatasetDefinition(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  /**
   * Reconfigure a dataset instance. Delegates to {@link Reconfigurable#reconfigure} if the dataset definition
   * implements that interface, and delegates to {@link #configure} otherwise.
   * @param def the dataset definition that will perform the (re)configure
   * @param name name of the dataset instance to reconfigure
   * @param newProps the updated dataset properties
   * @param currentSpec the current dataset specification
   * @return a new dataset specification representing the updated properties
   * @throws IncompatibleUpdateException if the updated properties are incompatible with the existing properties
   */
  public static DatasetSpecification reconfigure(DatasetDefinition def, String name,
                                                 DatasetProperties newProps, DatasetSpecification currentSpec)
    throws IncompatibleUpdateException {

    return def instanceof Reconfigurable
      ? ((Reconfigurable) def).reconfigure(name, newProps, currentSpec)
      : def.configure(name, newProps);
  }
}
