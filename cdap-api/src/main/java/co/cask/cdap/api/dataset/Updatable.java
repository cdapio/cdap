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

import java.io.IOException;

/**
 * Interface implemented by DatasetAdmin that have a way to update the dataset after reconfiguration.
 * It is optional for a DatasetAdmin to implement this interface; if not implemented, the dataset system
 * will assume that this dataset does need any actions to be performed on update (other than updating its spec).
 */
public interface Updatable {

  /**
   * Updates the dataset instance after it has been reconfigured. This method that will be called during
   * dataset update, on a {@link DatasetAdmin} that was created using the new dataset spec, before that
   * new spec is saved. That is, if this method fails, then the update fails and no dataset metadata has
   * been changed.
   *
   * @param oldSpec the specification of the dataset before reconfiguration
   */
  void update(DatasetSpecification oldSpec) throws IOException;
}
