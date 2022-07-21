/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package io.cdap.cdap.test;

import com.google.common.base.Throwables;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.client.DatasetClient;
import io.cdap.cdap.common.DatasetAlreadyExistsException;
import io.cdap.cdap.common.DatasetNotFoundException;
import io.cdap.cdap.common.DatasetTypeNotFoundException;
import io.cdap.cdap.proto.DatasetInstanceConfiguration;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;

import java.io.IOException;

/**
 * Remote implementation of {@link DatasetAdmin}.
 */
public final class RemoteDatasetAdmin implements DatasetAdmin {

  private final DatasetClient datasetClient;
  private final DatasetId datasetInstance;
  private final DatasetInstanceConfiguration dsConfiguration;

  public RemoteDatasetAdmin(DatasetClient datasetClient, DatasetId datasetInstance,
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
    } catch (UnauthenticatedException | UnauthorizedException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void create() throws IOException {
    try {
      datasetClient.create(datasetInstance, dsConfiguration);
    } catch (DatasetTypeNotFoundException | DatasetAlreadyExistsException | UnauthenticatedException
      | UnauthorizedException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void drop() throws IOException {
    try {
      datasetClient.delete(datasetInstance);
    } catch (DatasetNotFoundException | UnauthenticatedException | UnauthorizedException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void truncate() throws IOException {
    try {
      datasetClient.truncate(datasetInstance);
    } catch (UnauthenticatedException | UnauthorizedException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void upgrade() throws IOException {
    throw new UnsupportedOperationException(
      "Dataset upgrade is not supported on " + RemoteDatasetAdmin.class.getSimpleName() + ".");
  }
}
