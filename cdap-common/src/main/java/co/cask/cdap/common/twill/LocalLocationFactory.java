/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package co.cask.cdap.common.twill;

import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.File;
import java.net.URI;

/**
 * A {@link LocationFactory} for creating local file {@link Location}.
 */
// TODO: This is a copy of twill class, Remove this class after updating twill version to 0.7.0 - CDAP-4408
public class LocalLocationFactory implements LocationFactory {

  private final File basePath;

  /**
   * Constructs a LocalLocationFactory that Location created will be relative to system root.
   */
  public LocalLocationFactory() {
    this(new File("/"));
  }

  public LocalLocationFactory(File basePath) {
    this.basePath = basePath;
  }

  @Override
  public Location create(String path) {
    return new LocalLocation(this, new File(basePath, path));
  }

  @Override
  public Location create(URI uri) {
    if (uri.isAbsolute()) {
      return new LocalLocation(this, new File(uri));
    }
    return new LocalLocation(this, new File(basePath, uri.getPath()));
  }

  @Override
  public Location getHomeLocation() {
    return new LocalLocation(this, new File(System.getProperty("user.home")));
  }
}
