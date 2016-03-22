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

package co.cask.cdap.security.authorization;

import co.cask.cdap.api.Admin;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.InstanceNotFoundException;

/**
 * A no-op implementation of {@link Admin} for use in tests
 */
public class NoOpAdmin implements Admin {
  @Override
  public boolean datasetExists(String name) throws DatasetManagementException {
    return false;
  }

  @Override
  public String getDatasetType(String name) throws DatasetManagementException {
    throw new InstanceNotFoundException(name);
  }

  @Override
  public DatasetProperties getDatasetProperties(String name) throws DatasetManagementException {
    throw new InstanceNotFoundException(name);
  }

  @Override
  public void createDataset(String name, String type, DatasetProperties properties) throws DatasetManagementException {
    //no-op
  }

  @Override
  public void updateDataset(String name, DatasetProperties properties) throws DatasetManagementException {
    //no-op
  }

  @Override
  public void dropDataset(String name) throws DatasetManagementException {
    //no-op
  }

  @Override
  public void truncateDataset(String name) throws DatasetManagementException {
    //no-op
  }
}
