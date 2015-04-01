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
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.proto.Id;
import com.google.common.base.Joiner;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.File;
import java.io.IOException;

/**
 * Administration for file sets.
 */
public class FileSetAdmin implements DatasetAdmin {

  private final Location baseLocation;

  public FileSetAdmin(DatasetContext datasetContext, CConfiguration cConf,
                      NamespacedLocationFactory namespacedLocationFactory,
                      DatasetSpecification spec) throws IOException {
    String namespace = datasetContext.getNamespaceId();
    String dataDir = cConf.get(Constants.Dataset.DATA_DIR, Constants.Dataset.DEFAULT_DATA_DIR);
    String fileSetBasePath = FileSetDataset.determineBasePath(spec);
    this.baseLocation = namespacedLocationFactory.get(Id.Namespace.from(namespace)).append(dataDir)
      .append(fileSetBasePath);
  }

  @Override
  public boolean exists() throws IOException {
    return baseLocation.isDirectory();
  }

  @Override
  public void create() throws IOException {
    baseLocation.mkdirs();
  }

  @Override
  public void drop() throws IOException {
    baseLocation.delete(true);
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
