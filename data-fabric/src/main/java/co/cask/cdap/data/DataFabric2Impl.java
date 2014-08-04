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

package co.cask.cdap.data;

import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.net.URI;

/**
 * Simple implementation of the DataFabric interface.
 * TODO: will replace DataFabricImpl when integration with txds2 refactoring is done or will go away completely
 */
public class DataFabric2Impl implements DataFabric {

  private LocationFactory locationFactory;

  public DataFabric2Impl(LocationFactory locationFactory) {
    this.locationFactory = locationFactory;
  }

  @Override
  public Location getLocation(String path) throws IOException {
    return this.locationFactory.create(path);
  }

  @Override
  public Location getLocation(URI uri) throws IOException {
    return this.locationFactory.create(uri);
  }
}
