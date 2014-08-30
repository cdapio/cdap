/*
 * Copyright 2014 Cask, Inc.
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

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.data.runtime.DatasetClassLoaderUtil;

import java.io.IOException;

/**
 * Wrapper class to hold {@link co.cask.cdap.api.dataset.DatasetAdmin} and perform cleanup after the usage.
 */
public class DatasetAdminWrapper {

  private DatasetClassLoaderUtil datasetClassLoaderUtil;
  private DatasetAdmin datasetAdmin;

  public DatasetAdminWrapper(DatasetClassLoaderUtil datasetClassLoaderUtil, DatasetAdmin datasetAdmin) {
    this.datasetClassLoaderUtil = datasetClassLoaderUtil;
    this.datasetAdmin = datasetAdmin;
  }
  public DatasetAdmin getDatasetAdmin() {
    return datasetAdmin;
  }

  public void cleanup() throws IOException {
    if (datasetClassLoaderUtil != null) {
      datasetClassLoaderUtil.cleanup();
    }
  }
}

