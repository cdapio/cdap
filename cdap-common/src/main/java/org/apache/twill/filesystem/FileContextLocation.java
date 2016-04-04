/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.filesystem;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HAUtil;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * An implementation of {@link Location} using {@link FileContext}.
 * TODO (CDAP-5328): This class is copied from Twill for the fix in TWILL-163.
 * Should be removed when toString() is overridden in Twill (TWILL-167)
 */
final class FileContextLocation implements Location {

  private final FileContextLocationFactory locationFactory;
  private final FileContext fc;
  private final Path path;

  FileContextLocation(FileContextLocationFactory locationFactory, FileContext fc, Path path) {
    this.locationFactory = locationFactory;
    this.fc = fc;
    this.path = path;
  }

  @Override
  public boolean exists() throws IOException {
    return fc.util().exists(path);
  }

  @Override
  public String getName() {
    return path.getName();
  }

  @Override
  public boolean createNew() throws IOException {
    try {
      fc.create(path, EnumSet.of(CreateFlag.CREATE), Options.CreateOpts.createParent()).close();
      return true;
    } catch (FileAlreadyExistsException e) {
      return false;
    }
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return fc.open(path);
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    return fc.create(path, EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE), Options.CreateOpts.createParent());
  }

  @Override
  public OutputStream getOutputStream(String permission) throws IOException {
    return fc.create(path, EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE),
                     Options.CreateOpts.perms(new FsPermission(permission)),
                     Options.CreateOpts.createParent());
  }

  @Override
  public Location append(String child) throws IOException {
    if (child.startsWith("/")) {
      child = child.substring(1);
    }
    return new FileContextLocation(locationFactory, fc, new Path(URI.create(path.toUri() + "/" + child)));
  }

  @Override
  public Location getTempFile(String suffix) throws IOException {
    Path path = new Path(
      URI.create(this.path.toUri() + "." + UUID.randomUUID() + (suffix == null ? TEMP_FILE_SUFFIX : suffix)));
    return new FileContextLocation(locationFactory, fc, path);
  }

  @Override
  public URI toURI() {
    // In HA mode, the path URI returned by path created through FileContext is incompatible with the FileSystem,
    // which is used inside Hadoop. It is due to the fact that FileContext is not HA aware and it always
    // append "port" to the path URI, while the DistributedFileSystem always use the cluster logical
    // name, which doesn't allow having port in it.
    URI uri = path.toUri();
    if (HAUtil.isLogicalUri(locationFactory.getConfiguration(), uri)) {
      try {
        // Need to strip out the port if in HA
        return new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(),
                       -1, uri.getPath(), uri.getQuery(), uri.getFragment());
      } catch (URISyntaxException e) {
        // Shouldn't happen
        throw Throwables.propagate(e);
      }
    }

    return uri;
  }

  @Override
  public boolean delete() throws IOException {
    return delete(false);
  }

  @Override
  public boolean delete(boolean recursive) throws IOException {
    return fc.delete(path, recursive);
  }

  @Nullable
  @Override
  public Location renameTo(Location destination) throws IOException {
    Path targetPath = new Path(destination.toURI());
    try {
      fc.rename(path, targetPath, Options.Rename.OVERWRITE);
      return new FileContextLocation(locationFactory, fc, targetPath);
    } catch (FileAlreadyExistsException | FileNotFoundException | ParentNotDirectoryException e) {
      return null;
    }
  }

  @Override
  public boolean mkdirs() throws IOException {
    try {
      fc.mkdir(path, null, true);
      return true;
    } catch (FileAlreadyExistsException e) {
      return false;
    }
  }

  @Override
  public long length() throws IOException {
    return fc.getFileStatus(path).getLen();
  }

  @Override
  public long lastModified() throws IOException {
    return fc.getFileStatus(path).getModificationTime();
  }

  @Override
  public boolean isDirectory() throws IOException {
    try {
      return fc.getFileStatus(path).isDirectory();
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  @Override
  public List<Location> list() throws IOException {
    RemoteIterator<FileStatus> statuses = fc.listStatus(path);
    ImmutableList.Builder<Location> result = ImmutableList.builder();
    while (statuses.hasNext()) {
      FileStatus status = statuses.next();
      if (!Objects.equals(path, status.getPath())) {
        result.add(new FileContextLocation(locationFactory, fc, status.getPath()));
      }
    }
    return result.build();

  }

  @Override
  public LocationFactory getLocationFactory() {
    return locationFactory;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FileContextLocation that = (FileContextLocation) o;
    return Objects.equals(path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path);
  }

  @Override
  public String toString() { return toURI().toString(); }
}
