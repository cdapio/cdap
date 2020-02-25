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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * A {@link Location} implementation that caches data read locally to allow efficient re-reading.
 */
final class CachingLocation implements Location {

  private static final Logger LOG = LoggerFactory.getLogger(CachingLocation.class);

  private final LocationFactory locationFactory;
  private final Location delegate;
  private final CachingPathProvider cachingPathProvider;

  CachingLocation(LocationFactory locationFactory, Location delegate, CachingPathProvider cachingPathProvider) {
    this.locationFactory = locationFactory;
    this.delegate = delegate;
    this.cachingPathProvider = cachingPathProvider;
  }

  @Override
  public boolean exists() throws IOException {
    return delegate.exists();
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public boolean createNew() throws IOException {
    return delegate.createNew();
  }

  @Override
  public boolean createNew(String permission) throws IOException {
    return delegate.createNew(permission);
  }

  @Override
  public String getPermissions() throws IOException {
    return delegate.getPermissions();
  }

  @Override
  public String getOwner() throws IOException {
    return delegate.getOwner();
  }

  @Override
  public String getGroup() throws IOException {
    return delegate.getGroup();
  }

  @Override
  public void setGroup(String group) throws IOException {
    delegate.setGroup(group);
  }

  @Override
  public void setPermissions(String permission) throws IOException {
    delegate.setPermissions(permission);
  }

  @Override
  public InputStream getInputStream() throws IOException {
    Path cachePath = cachingPathProvider.apply(delegate).orElse(null);
    if (cachePath == null) {
      return delegate.getInputStream();
    }

    try {
      return new FileInputStream(cachePath.toFile());
    } catch (IOException e) {
      // If fail to open stream from the cache, try to regenerate it.
      Files.deleteIfExists(cachePath);
    }

    try (InputStream input = delegate.getInputStream()) {
      Files.createDirectories(cachePath.getParent());
      Path tmpPath = Files.createTempFile(cachePath.getParent(), cachePath.getFileName().toString(), ".tmp",
                                          PosixFilePermissions.asFileAttribute(
                                            PosixFilePermissions.fromString("rw-------")));
      Files.copy(input, tmpPath, StandardCopyOption.REPLACE_EXISTING);
      try {
        Files.move(tmpPath, cachePath, StandardCopyOption.ATOMIC_MOVE);
      } catch (FileAlreadyExistsException e) {
        // Ignore because the target cache path is already exist. This can happen when there is concurrent fetches.
        LOG.trace("Cache file already exists", e);
      }
      LOG.debug("Cached location {} to {}", delegate, cachePath);
      return new FileInputStream(cachePath.toFile());
    } catch (IOException e) {
      LOG.warn("Failed to cache location {}", delegate, e);
      return delegate.getInputStream();
    }
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    return delegate.getOutputStream();
  }

  @Override
  public OutputStream getOutputStream(String permission) throws IOException {
    return delegate.getOutputStream(permission);
  }

  @Override
  public Location append(String child) throws IOException {
    return new CachingLocation(locationFactory, delegate.append(child), cachingPathProvider);
  }

  @Override
  public Location getTempFile(String suffix) throws IOException {
    return new CachingLocation(locationFactory, delegate.getTempFile(suffix), cachingPathProvider);
  }

  @Override
  public URI toURI() {
    return delegate.toURI();
  }

  @Override
  public boolean delete() throws IOException {
    return delegate.delete();
  }

  @Override
  public boolean delete(boolean recursive) throws IOException {
    return delegate.delete(recursive);
  }

  @Override
  @Nullable
  public Location renameTo(Location destination) throws IOException {
    Location result = this.delegate.renameTo(destination);
    return result == null ? null : new CachingLocation(locationFactory, result, cachingPathProvider);
  }

  @Override
  public boolean mkdirs() throws IOException {
    return delegate.mkdirs();
  }

  @Override
  public boolean mkdirs(String permission) throws IOException {
    return delegate.mkdirs(permission);
  }

  @Override
  public long length() throws IOException {
    return delegate.length();
  }

  @Override
  public long lastModified() throws IOException {
    return delegate.lastModified();
  }

  @Override
  public boolean isDirectory() throws IOException {
    return delegate.isDirectory();
  }

  @Override
  public List<Location> list() throws IOException {
    return delegate.list();
  }

  @Override
  public LocationFactory getLocationFactory() {
    return locationFactory;
  }

  @Override
  public String toString() {
    return delegate.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CachingLocation that = (CachingLocation) o;
    return Objects.equals(delegate, that.delegate);
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }
}
