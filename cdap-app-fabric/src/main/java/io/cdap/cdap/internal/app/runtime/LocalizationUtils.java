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

package io.cdap.cdap.internal.app.runtime;

import com.google.common.io.Files;
import com.google.common.io.Resources;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.internal.app.runtime.distributed.LocalizeResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Paths;

/**
 * Utilities for file localization.
 */
public final class LocalizationUtils {
  private static final Logger LOG = LoggerFactory.getLogger(LocalizationUtils.class);

  /**
   * Localizes the specified {@link LocalizeResource} in the specified {@link File targetDir} with the specified
   * file name and returns the {@link File} pointing to the localized file.
   *
   * @param fileName the name to localize the file with
   * @param resource the {@link LocalizeResource} to localize
   * @param targetDir the directory to localize the resource in
   * @return the {@link File} pointing to the localized file.
   */
  public static File localizeResource(String fileName, LocalizeResource resource, File targetDir) throws IOException {
    File localizedResource = new File(targetDir, fileName);
    File input = getFileToLocalize(resource, targetDir);
    if (resource.isArchive()) {
      LOG.debug("Decompress file {} to {}", input, localizedResource);
      Locations.unpack(Locations.toLocation(input), localizedResource);
    } else {
      try {
        LOG.debug("Hard link file from {} to {}", input, localizedResource);
        java.nio.file.Files.createLink(Paths.get(localizedResource.toURI()), Paths.get(input.toURI()));
      } catch (Exception e) {
        LOG.debug("Copy file from {} to {}", input, localizedResource);
        Files.copy(input, localizedResource);
      }
    }
    return localizedResource;
  }

  /**
   * Returns a local {@link File} for the specified {@link LocalizeResource}. If the specified {@link LocalizeResource}
   * points to a local file already, it simply returns a {@link File} object for it. If it is not local, then the
   * method will try to download the resource to a local temporary file and return it.
   *
   * @param resource the {@link LocalizeResource} for which a local file is requested
   * @param tempDir the {@link File directory} to download the file to if it is a remote file
   * @return a local {@link File} for the specified resource
   */
  private static File getFileToLocalize(LocalizeResource resource, File tempDir) throws IOException {
    URI uri = resource.getURI();
    if ("file".equals(uri.getScheme())) {
      // Local file. Just return a File object for the file.
      return new File(uri.getPath());
    }
    URL url = uri.toURL();
    String name = new File(uri.getPath()).getName();
    File tempFile = new File(tempDir, name);
    Files.copy(Resources.newInputStreamSupplier(url), tempFile);
    return tempFile;
  }

  /**
   * Get the filename part from a {@link URI} to be localized.
   * For a URI "file:///tmp/foo.jar", "foo.jar" will be returned.
   * For a URI "file:///tmp/foo.jar#bar.jar", "bar.jar" will be returned.
   *
   * @param uri The {@link URI} to get filename from.
   * @return filename to be localized.
   */
  public static String getLocalizedName(URI uri) {
    String localizedName = uri.getFragment();
    if (localizedName == null) {
      String path = uri.getPath();
      int idx = path.lastIndexOf("/");
      localizedName = idx >= 0 ? path.substring(idx + 1) : path;
    }
    return localizedName;
  }

  private LocalizationUtils() {
  }
}
