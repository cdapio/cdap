/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.k8s.runtime.localizer;

import com.google.common.io.Resources;
import org.apache.twill.api.LocalFile;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.DefaultLocalFile;
import org.apache.twill.internal.utils.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import javax.annotation.Nullable;

/**
 * Places files on at a distributed Location that the FileLocalizer can pull through a call back to app-fabric.
 */
public class LocationPreparer extends FilePreparer {
  private static final Logger LOG = LoggerFactory.getLogger(LocationPreparer.class);
  private final Location appLocation;
  private final String locationScheme;
  private final String modifiedScheme;

  public LocationPreparer(Location appLocation, @Nullable String modifiedScheme) {
    this.appLocation = appLocation;
    this.locationScheme = appLocation.toURI().getScheme();
    this.modifiedScheme = modifiedScheme;
  }

  @Override
  public LocalFile prepareFile(String runnableName, LocalFile localFile) throws IOException {
    Location location;
    URI uri = localFile.getURI();
    if (modifiedScheme == null && locationScheme.equals(uri.getScheme())) {
      // If the source file location is having the same scheme as the target location, no need to copy
      location = appLocation.getLocationFactory().create(uri);
    } else {
      URL url = uri.toURL();
      LOG.debug("Create and copy {} : {}", runnableName, url);
      // Preserves original suffix for expansion.
      location = copyFromURL(url, createTempLocation(Paths.addExtension(url.getFile(), localFile.getName())));
      LOG.debug("Done {} : {}", runnableName, url);
    }
    // this is primarily used if CDAP is configured to use the local filesystem, in which case the scheme will
    // be 'file'. This will be interpreted by the FileLocalizer as a local file in the container, instead of a file
    // that must be pulled from app-fabric.
    URI finalURI = location.toURI();
    if (modifiedScheme != null) {
      String scheme = finalURI.getScheme();
      finalURI = URI.create(modifiedScheme + finalURI.toString().substring(scheme.length()));
    }
    return new DefaultLocalFile(localFile.getName(), finalURI, location.lastModified(),
                                location.length(), localFile.isArchive(), localFile.getPattern());
  }

  @Override
  public RuntimeConfig prepareRuntimeConfig(Path configDir) throws IOException {
    Location location = createTempLocation(Constants.Files.RUNTIME_CONFIG_JAR);
    try (OutputStream outputStream = location.getOutputStream()) {
      writeJar(configDir, outputStream);
    }
    return new RuntimeConfig(location.toURI(), null, null);
  }

  private Location copyFromURL(URL url, Location target) throws IOException {
    try (OutputStream os = new BufferedOutputStream(target.getOutputStream())) {
      Resources.copy(url, os);
      return target;
    }
  }

  private Location createTempLocation(String fileName) throws IOException {
    String suffix = Paths.getExtension(fileName);
    String name = fileName.substring(0, fileName.length() - suffix.length() - 1);
    return appLocation.append(name).getTempFile('.' + suffix);
  }
}
