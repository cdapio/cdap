/*
 * Copyright © 2015 Cask Data, Inc.
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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A location that delegates all operations to another location. However, it can enforce read-only
 * access and other constraints, such as to prevent navigation outside of a base location.
 */
public class LimitedLocation implements Location {

  private final Location delegate;
  private final String baseURI;
  private final boolean readOnly;

  public LimitedLocation(Location location, boolean readOnly) {
    this(location, location.toURI().toString(), readOnly);
  }

  private LimitedLocation(Location delegate, String baseURI, boolean readOnly) {
    this.delegate = delegate;
    this.baseURI = baseURI;
    this.readOnly = readOnly;
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
    if (readOnly) {
      throw new IOException("Operation not permitted for read-only location '" + getName() + "'");
    }
    return delegate.createNew();
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return delegate.getInputStream();
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    if (readOnly) {
      throw new IOException("Operation not permitted for read-only location '" + getName() + "'");
    }
    return delegate.getOutputStream();
  }

  @Override
  public OutputStream getOutputStream(String s) throws IOException {
    if (readOnly) {
      throw new IOException("Operation not permitted for read-only location '" + getName() + "'");
    }
    return delegate.getOutputStream(s);
  }

  @Override
  public Location append(String s) throws IOException {
    return wrap(delegate.append(s));
  }

  @Override
  public Location getTempFile(String s) throws IOException {
    // TODO what does this mean? Should we just throw Unsupported?
    if (readOnly) {
      throw new IOException("Operation not permitted for read-only location '" + getName() + "'");
    }
    return wrap(delegate.getTempFile(s));
  }

  @Override
  public URI toURI() {
    return delegate.toURI();
  }

  @Override
  public boolean delete() throws IOException {
    if (readOnly) {
      throw new IOException("Operation not permitted for read-only location '" + getName() + "'");
    }
    return delegate.delete();
  }

  @Override
  public boolean delete(boolean b) throws IOException {
    if (readOnly) {
      throw new IOException("Operation not permitted for read-only location '" + getName() + "'");
    }
    return delegate.delete(b);
  }

  @Override
  @Nullable
  public Location renameTo(Location location) throws IOException {
    if (readOnly) {
      throw new IOException("Operation not permitted for read-only location '" + getName() + "'");
    }
    validateLocation(location);
    return wrap(delegate.renameTo(location));
  }

  @Override
  public boolean mkdirs() throws IOException {
    if (readOnly) {
      throw new IOException("Operation not permitted for read-only location '" + getName() + "'");
    }
    return delegate.mkdirs();
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
    return Lists.transform(delegate.list(), new Function<Location, Location>() {
      @Nullable
      @Override
      public Location apply(@Nullable Location loc) {
        return wrap(loc);
      }
    });
  }

  @Override
  public LocationFactory getLocationFactory() {
    return new LimitedLocationFactory(delegate.getLocationFactory());
  }

  private void validateLocation(Location location) {
    String uri = location.toURI().toString();
    if (!uri.startsWith(baseURI)) {
      throw new IllegalArgumentException(String.format(
        "Attempt to create location '%s' which is outside of base '%s'.", uri, baseURI));
    }
  }

  private Location wrap(Location location) {
    return new LimitedLocation(location, baseURI, readOnly);
  }

  /**
   * A location factory that delegates all operations to another location factory, but wraps the
   * resulting locations into a LimitedLocation for control of its operations.
   */
  public class LimitedLocationFactory implements LocationFactory {

    private final LocationFactory delegate;

    public LimitedLocationFactory(LocationFactory delegate) {
      this.delegate = delegate;
    }

    @Override
    public Location create(String path) {
      Location loc = delegate.create(path);
      validateLocation(loc);
      return wrap(loc);
    }

    @Override
    public Location create(URI uri) {
      Location loc = delegate.create(uri);
      validateLocation(loc);
      return wrap(loc);
    }

    @Override
    public Location getHomeLocation() {
      Location loc = delegate.getHomeLocation();
      validateLocation(loc);
      return wrap(loc);
    }
  }
}
