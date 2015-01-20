/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;

/**
 * Administration for file datasets.
 */
public class FileAdmin implements DatasetAdmin {

  private final LocationFactory locationFactory;

  private final String basePath;

  public FileAdmin(LocationFactory locationFactory, DatasetSpecification spec) {
    this.locationFactory = locationFactory;
    this.basePath = FileSetProperties.getBasePath(spec.getProperties());
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
