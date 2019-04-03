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

package co.cask.cdap.data2.dataset2.lib.cube;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.Updatable;
import co.cask.cdap.api.dataset.lib.CompositeDatasetAdmin;

import java.io.IOException;
import java.util.Map;

class CubeDatasetAdmin extends CompositeDatasetAdmin {

  private final DatasetSpecification spec;

  CubeDatasetAdmin(DatasetSpecification spec, Map<String, ? extends DatasetAdmin> admins) {
    super(admins);
    this.spec = spec;
  }

  @Override
  public void update(DatasetSpecification oldSpec) throws IOException {
    // update all existing resolution tables, create all new resolutions
    for (Map.Entry<String, DatasetSpecification> entry : spec.getSpecifications().entrySet()) {
      DatasetSpecification oldSubSpec = spec.getSpecification(entry.getKey());
      DatasetAdmin subAdmin = delegates.get(entry.getKey());
      if (oldSubSpec != null && subAdmin instanceof Updatable) {
        ((Updatable) subAdmin).update(oldSubSpec);
      } else if (oldSubSpec == null) {
        subAdmin.create();
      }
    }
    // TODO (CDAP-6342) delete all resolutions that were removed as part of the update
  }
}
