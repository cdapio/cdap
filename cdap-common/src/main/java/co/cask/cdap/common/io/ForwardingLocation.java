/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.common.io;

import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A {@link Location} implementation that always forward to another {@link Location}.
 */
public abstract class ForwardingLocation implements Location {

  private final Location delegate;

  protected ForwardingLocation(Location delegate) {
    this.delegate = delegate;
  }

  public final Location getDelegate() {
    return delegate;
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
  public InputStream getInputStream() throws IOException {
    return delegate.getInputStream();
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
  public String getPermissions() throws IOException {
    return delegate.getPermissions();
  }

  @Override
  public void setPermissions(String permission) throws IOException {
    delegate.setPermissions(permission);
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
    return delegate.append(child);
  }

  @Override
  public Location getTempFile(String suffix) throws IOException {
    return delegate.getTempFile(suffix);
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
    return delegate.renameTo(destination);
  }

  @Override
  public boolean mkdirs() throws IOException {
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
    return delegate.list();
  }

  @Override
  public LocationFactory getLocationFactory() {
    return delegate.getLocationFactory();
  }

  @Override
  public String toString() {
    return delegate.toString();
  }
}
