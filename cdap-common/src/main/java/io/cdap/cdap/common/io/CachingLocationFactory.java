/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.common.io;

import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.InputStream;
import java.net.URI;

/**
 * A {@link LocationFactory} implementation that caches content of {@link InputStream} opened from
 * {@link Location} locally.
 */
public class CachingLocationFactory implements LocationFactory {

  private final LocationFactory delegate;
  private final CachingPathProvider cachingPathProvider;

  public CachingLocationFactory(LocationFactory delegate, CachingPathProvider cachingPathProvider) {
    this.delegate = delegate;
    this.cachingPathProvider = cachingPathProvider;
  }

  @Override
  public Location create(String path) {
    return new CachingLocation(this, delegate.create(path), cachingPathProvider);
  }

  @Override
  public Location create(URI uri) {
    return new CachingLocation(this, delegate.create(uri), cachingPathProvider);
  }

  @Override
  public Location getHomeLocation() {
    return new CachingLocation(this, delegate.getHomeLocation(), cachingPathProvider);
  }
}
