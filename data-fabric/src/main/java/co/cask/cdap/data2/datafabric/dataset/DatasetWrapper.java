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

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.data.runtime.DatasetClassLoaderUtil;

import java.io.IOException;

/**
 * Wrapper class for Type that extends {@link co.cask.cdap.api.dataset.Dataset}
 * @param <D>
 */
public class DatasetWrapper<D extends Dataset> {
  private DatasetClassLoaderUtil datasetClassLoaderUtil;
  private  D  dataset;

  public DatasetWrapper(DatasetClassLoaderUtil datasetClassLoaderUtil, D dataset) {
    this.datasetClassLoaderUtil = datasetClassLoaderUtil;
    this.dataset = dataset;
  }

  public D getDataset() {
    return dataset;
  }

  public void cleanup() throws IOException {
    if (datasetClassLoaderUtil != null) {
      datasetClassLoaderUtil.cleanup();
    }
  }
}
