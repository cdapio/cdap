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

package io.cdap.cdap.data2.dataset2;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.common.DatasetNotFoundException;
import io.cdap.cdap.common.entity.EntityExistenceVerifier;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;

/**
 * {@link EntityExistenceVerifier} for {@link DatasetId datasets}.
 */
public class DatasetExistenceVerifier implements EntityExistenceVerifier<DatasetId> {
  private final DatasetFramework dsFramework;

  @Inject
  DatasetExistenceVerifier(DatasetFramework dsFramework) {
    this.dsFramework = dsFramework;
  }

  @Override
  public void ensureExists(DatasetId datasetId) throws DatasetNotFoundException, UnauthorizedException {
    try {
      if (!dsFramework.hasInstance(datasetId)) {
        throw new DatasetNotFoundException(datasetId);
      }
    } catch (DatasetManagementException e) {
      throw Throwables.propagate(e);
    }
  }
}
