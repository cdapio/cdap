/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.data;

import com.continuuity.data2.dataset.api.DataSetManager;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * This is the abstract base class for data fabric.
 */
public interface DataFabric {

  // These are to support new TxDs2 system. DataFabric will go away once we fully migrate to it.
  <T> T getDataSetClient(String name, Class<? extends T> type, @Nullable Properties props);

  <T> DataSetManager getDataSetManager(Class<? extends T> type);

  // Provides access to filesystem

  /**
   * @param path The path representing the location.
   * @return a {@link Location} object to access given path.
   * @throws java.io.IOException
   */
  Location getLocation(String path) throws IOException;

  /**
   * @param path The path representing the location.
   * @return a {@link Location} object to access given path.
   * @throws IOException
   */
  Location getLocation(URI path) throws IOException;
}
