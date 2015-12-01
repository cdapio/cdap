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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.internal.app.runtime.distributed.LocalizeResource;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;

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
      unpack(input, localizedResource);
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

  private static void unpack(File archive, File targetDir) throws IOException {
    if (!targetDir.exists()) {
      //noinspection ResultOfMethodCallIgnored
      targetDir.mkdir();
    }
    String extension = Files.getFileExtension(archive.getPath()).toLowerCase();
    switch (extension) {
      case "zip":
      case "jar":
        BundleJarUtil.unJar(Locations.toLocation(archive), targetDir);
        break;
      case "gz":
        // gz is not recommended for archiving multiple files together. So we only support .tar.gz
        Preconditions.checkArgument(archive.getName().endsWith(".tar.gz"), "'.gz' format is not supported for " +
          "archiving multiple files. Please use 'zip', 'jar', '.tar.gz', 'tgz' or 'tar'.");
        untargz(archive, targetDir);
        break;
      case "tgz":
        untargz(archive, targetDir);
        break;
      case "tar":
        untar(archive, targetDir);
        break;
      default:
        throw new IllegalArgumentException(String.format("Unsupported compression type '%s'. Only 'zip', 'jar', " +
                                                           "'tar.gz', 'tgz' and 'tar'  are supported.",
                                                         extension));
    }
  }

  private static void untar(File tarFile, File targetDir) throws IOException {
    try (TarArchiveInputStream tis = new TarArchiveInputStream(new FileInputStream(tarFile))) {
      extractTar(tis, targetDir);
    }
  }

  private static void untargz(File tarGzFile, File targetDir) throws IOException {
    try (TarArchiveInputStream tis = new TarArchiveInputStream(new GZIPInputStream(new FileInputStream(tarGzFile)))) {
      extractTar(tis, targetDir);
    }
  }

  private static void extractTar(final TarArchiveInputStream tis, File targetDir) throws IOException {
    TarArchiveEntry entry = tis.getNextTarEntry();
    while (entry != null) {
      File output = new File(targetDir, new File(entry.getName()).getName());
      if (entry.isDirectory()) {
        //noinspection ResultOfMethodCallIgnored
        output.mkdirs();
      } else {
        //noinspection ResultOfMethodCallIgnored
        output.getParentFile().mkdirs();
        ByteStreams.copy(tis, Files.newOutputStreamSupplier(output));
      }
      entry = tis.getNextTarEntry();
    }
  }

  private LocalizationUtils() {
  }
}
