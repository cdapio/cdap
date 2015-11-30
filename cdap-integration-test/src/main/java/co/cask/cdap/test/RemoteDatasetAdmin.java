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

package co.cask.cdap.test;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.client.DatasetClient;
import co.cask.cdap.common.DatasetAlreadyExistsException;
import co.cask.cdap.common.DatasetNotFoundException;
import co.cask.cdap.common.DatasetTypeNotFoundException;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.proto.DatasetInstanceConfiguration;
import co.cask.cdap.proto.Id;
import com.google.common.base.Throwables;

import java.io.IOException;

/**
 * Remote implementation of {@link DatasetAdmin}.
 */
public final class RemoteDatasetAdmin implements DatasetAdmin {

  private final DatasetClient datasetClient;
  private final Id.DatasetInstance datasetInstance;
  private final DatasetInstanceConfiguration dsConfiguration;

  public RemoteDatasetAdmin(DatasetClient datasetClient, Id.DatasetInstance datasetInstance,
                            DatasetInstanceConfiguration dsConfiguration) {
    this.datasetClient = datasetClient;
    this.datasetInstance = datasetInstance;
    this.dsConfiguration = dsConfiguration;
  }

  @Override
  public void close() throws IOException {
    // nothing needed to close
  }

  @Override
  public boolean exists() throws IOException {
    try {
      return datasetClient.exists(datasetInstance);
    } catch (UnauthorizedException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void create() throws IOException {
    try {
      datasetClient.create(datasetInstance, dsConfiguration);
    } catch (DatasetTypeNotFoundException | DatasetAlreadyExistsException | UnauthorizedException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void drop() throws IOException {
    try {
      datasetClient.delete(datasetInstance);
    } catch (DatasetNotFoundException | UnauthorizedException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void truncate() throws IOException {
    try {
      datasetClient.truncate(datasetInstance);
    } catch (UnauthorizedException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void upgrade() throws IOException {
    throw new UnsupportedOperationException(
      "Dataset upgrade is not supported on " + RemoteDatasetAdmin.class.getSimpleName() + ".");
  }
}
