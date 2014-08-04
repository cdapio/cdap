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

import java.io.IOException;
import java.net.URI;

/**
 * This is the abstract base class for data fabric.
 */
public interface DataFabric {

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
