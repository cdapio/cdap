/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.common.lang.jar;

import co.cask.cdap.common.io.Locations;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import org.apache.twill.filesystem.Location;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Utility functions that operate on bundle jars.
 */
// TODO: remove this -- not sure how to refactor though
public class BundleJarUtil {

  /**
   * Load the manifest inside the given jar.
   *
   * @param jarLocation Location of the jar file.
   * @return The manifest inside the jar file or {@code null} if no manifest inside the jar file.
   * @throws IOException if failed to load the manifest.
   */
  public static Manifest getManifest(Location jarLocation) throws IOException {
    URI uri = jarLocation.toURI();

    // Small optimization if the location is local
    if ("file".equals(uri.getScheme())) {
      JarFile jarFile = new JarFile(new File(uri));
      try {
        return jarFile.getManifest();
      } finally {
        jarFile.close();
      }
    }

    // Otherwise, need to search it with JarInputStream
    JarInputStream is = new JarInputStream(new BufferedInputStream(jarLocation.getInputStream()));
    try {
      // This only looks at the first entry, which if is created with jar util, then it'll be there.
      Manifest manifest = is.getManifest();
      if (manifest != null) {
        return manifest;
      }

      // Otherwise, slow path. Need to goes through the entries
      JarEntry jarEntry = is.getNextJarEntry();
      while (jarEntry != null) {
        if (JarFile.MANIFEST_NAME.equals(jarEntry.getName())) {
          return new Manifest(is);
        }
        jarEntry = is.getNextJarEntry();
      }

    } finally {
      is.close();
    }

    return null;
  }

  /**
   * Returns an {@link InputSupplier} for a given entry. This avoids unjar the whole file to just get one entry.
   * However, to get many entries, unjar would be more efficient. Also, the jar file is scanned every time the
   * {@link InputSupplier#getInput()} is invoked.
   *
   * @param jarLocation Location of the jar file.
   * @param entryName Name of the entry to fetch
   * @return An {@link InputSupplier}.
   */
  public static InputSupplier<InputStream> getEntry(final Location jarLocation,
                                                    final String entryName) throws IOException {
    Preconditions.checkArgument(jarLocation != null);
    Preconditions.checkArgument(entryName != null);
    final URI uri = jarLocation.toURI();

    // Small optimization if the location is local
    if ("file".equals(uri.getScheme())) {
      return new InputSupplier<InputStream>() {

        @Override
        public InputStream getInput() throws IOException {
          final JarFile jarFile = new JarFile(new File(uri));
          ZipEntry entry = jarFile.getEntry(entryName);
          if (entry == null) {
            throw new IOException("Entry not found for " + entryName);
          }
          return new FilterInputStream(jarFile.getInputStream(entry)) {
            @Override
            public void close() throws IOException {
              try {
                super.close();
              } finally {
                jarFile.close();
              }
            }
          };
        }
      };
    }

    // Otherwise, use JarInputStream
    return new InputSupplier<InputStream>() {
      @Override
      public InputStream getInput() throws IOException {
        JarInputStream is = new JarInputStream(jarLocation.getInputStream());
        JarEntry entry = is.getNextJarEntry();
        while (entry != null) {
          if (entryName.equals(entry.getName())) {
            return is;
          }
          entry = is.getNextJarEntry();
        }
        Closeables.closeQuietly(is);
        throw new IOException("Entry not found for " + entryName);
      }
    };
  }

  /**
   * Unpack a jar file in the given location to a directory.
   *
   * @param jarLocation Location containing the jar file
   * @param destinationFolder Directory to expand into
   * @return The {@code destinationFolder}
   * @throws IOException If failed to expand the jar
   */
  public static File unpackProgramJar(Location jarLocation, File destinationFolder) throws IOException {
    Preconditions.checkArgument(jarLocation != null);
    return unpackProgramJar(Locations.newInputSupplier(jarLocation), destinationFolder);
  }

  /**
   * Unpack a jar source to a directory.
   *
   * @param inputSupplier Supplier for the jar source
   * @param destinationFolder Directory to expand into
   * @return The {@code destinationFolder}
   * @throws IOException If failed to expand the jar
   */
  public static File unpackProgramJar(InputSupplier<? extends InputStream> inputSupplier,
                                      File destinationFolder) throws IOException {
    Preconditions.checkArgument(inputSupplier != null);
    Preconditions.checkArgument(destinationFolder != null);
    Preconditions.checkArgument(destinationFolder.canWrite());

    destinationFolder.mkdirs();
    Preconditions.checkState(destinationFolder.exists());
    ZipInputStream input = new ZipInputStream(inputSupplier.getInput());
    try {
      unJar(input, destinationFolder);
      return destinationFolder;
    } finally {
      Closeables.closeQuietly(input);
    }
  }

  private static void unJar(ZipInputStream input, File targetDirectory) throws IOException {
    ZipEntry entry;
    while ((entry = input.getNextEntry()) != null) {
      File output = new File(targetDirectory, entry.getName());

      if (entry.isDirectory()) {
        output.mkdirs();
      } else {
        output.getParentFile().mkdirs();
        ByteStreams.copy(input, Files.newOutputStreamSupplier(output));
      }
    }
  }

  private BundleJarUtil() {
  }
}
