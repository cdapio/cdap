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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;

import java.net.URI;
import java.util.Objects;

/**
 * A {@link LocationFactory} implementation that uses {@link FileContext} to create {@link Location}.
 *
 * TODO (CDAP-4923): This class is copied from Twill for the fix in TWILL-163.
 * Should be removed when upgraded to twill-0.8.0
 */
public class FileContextLocationFactory implements LocationFactory {

  private final Configuration configuration;
  private final FileContext fc;
  private final Path pathBase;

  /**
   * Same as {@link #FileContextLocationFactory(Configuration, String) FileContextLocationFactory(configuration, "/")}.
   */
  public FileContextLocationFactory(Configuration configuration) {
    this(configuration, "/");
  }

  /**
   * Creates a new instance.
   *
   * @param configuration the hadoop configuration
   * @param pathBase base path for all non-absolute location created through this {@link LocationFactory}.
   */
  public FileContextLocationFactory(Configuration configuration, String pathBase) {
    this(configuration, createFileContext(configuration), pathBase);
  }

  /**
   * Creates a new instance with the given {@link FileContext} created from the given {@link Configuration}.
   *
   * @param configuration the hadoop configuration
   * @param fc {@link FileContext} instance created from the given configuration
   * @param pathBase base path for all non-absolute location created through this (@link LocationFactory}.
   */
  public FileContextLocationFactory(Configuration configuration, FileContext fc, String pathBase) {
    this.configuration = configuration;
    this.fc = fc;
    this.pathBase = new Path(pathBase.startsWith("/") ? pathBase : "/" + pathBase);
  }

  @Override
  public Location create(String path) {
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    Path locationPath;
    if (path.isEmpty()) {
      locationPath = pathBase;
    } else {
      locationPath = new Path(path);
    }
    locationPath = locationPath.makeQualified(fc.getDefaultFileSystem().getUri(), pathBase);
    return new FileContextLocation(this, fc, locationPath);
  }

  @Override
  public Location create(URI uri) {
    URI contextURI = fc.getWorkingDirectory().toUri();
    if (Objects.equals(contextURI.getScheme(), uri.getScheme())
      && Objects.equals(contextURI.getAuthority(), uri.getAuthority())) {
      // A full URI
      return new FileContextLocation(this, fc, new Path(uri));
    }

    if (uri.isAbsolute()) {
      // Needs to be of the same scheme
      Preconditions.checkArgument(Objects.equals(contextURI.getScheme(), uri.getScheme()),
                                  "Only URI with '%s' scheme is supported", contextURI.getScheme());
      Path locationPath = new Path(uri).makeQualified(fc.getDefaultFileSystem().getUri(), pathBase);
      return new FileContextLocation(this, fc, locationPath);
    }

    return create(uri.getPath());
  }

  @Override
  public Location getHomeLocation() {
    // Fix for TWILL-163. FileContext.getHomeDirectory() uses System.getProperty("user.name") instead of UGI
    return new FileContextLocation(this, fc,
                                   new Path(fc.getHomeDirectory().getParent(), fc.getUgi().getShortUserName()));
  }

  /**
   * Returns the {@link FileContext} used by this {@link LocationFactory}.
   */
  public FileContext getFileContext() {
    return fc;
  }

  /**
   * Returns the {@link Configuration} used by this {@link LocationFactory}.
   */
  public Configuration getConfiguration() {
    return configuration;
  }

  private static FileContext createFileContext(Configuration configuration) {
    try {
      return FileContext.getFileContext(configuration);
    } catch (UnsupportedFileSystemException e) {
      throw Throwables.propagate(e);
    }
  }
}
