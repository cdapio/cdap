/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.file;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.DatasetNamespace;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;

/**
 * Administration for file datasets.
 */
public class FileAdmin implements DatasetAdmin {

  private final LocationFactory locationFactory;

  private final String basePath;

  public FileAdmin(DatasetContext datasetContext, CConfiguration cConf, LocationFactory locationFactory,
                   DatasetSpecification spec) {
    String namespace = datasetContext.getNamespaceId();
    this.locationFactory = locationFactory;
    String dataDir = cConf.get(Constants.Dataset.DATA_DIR, Constants.Dataset.DEFAULT_DATA_DIR);
    String basePath = FileSetProperties.getBasePath(spec.getProperties());
    this.basePath = String.format("%s/%s/%s", namespace, dataDir, basePath);
  }

  @Override
  public boolean exists() throws IOException {
    return locationFactory.create(basePath).isDirectory();
  }

  @Override
  public void create() throws IOException {
    locationFactory.create(basePath).mkdirs();
  }

  @Override
  public void drop() throws IOException {
    locationFactory.create(basePath).delete(true);
  }

  @Override
  public void truncate() throws IOException {
    drop();
    create();
  }

  @Override
  public void upgrade() throws IOException {
    // nothing to do
  }

  @Override
  public void close() throws IOException {
    // nothing to do
  }
}
