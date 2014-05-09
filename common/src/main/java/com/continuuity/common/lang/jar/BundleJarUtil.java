package com.continuuity.common.lang.jar;

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

  public static File unpackProgramJar(Location programJarLocation, File destinationFolder) throws IOException {
    Preconditions.checkArgument(programJarLocation != null);
    Preconditions.checkArgument(programJarLocation.exists());
    Preconditions.checkArgument(destinationFolder != null);
    Preconditions.checkArgument(destinationFolder.canWrite());

    destinationFolder.mkdirs();
    Preconditions.checkState(destinationFolder.exists());
    unJar(new ZipInputStream(programJarLocation.getInputStream()), destinationFolder);
    return destinationFolder;
  }

  private static void unJar(ZipInputStream jarInputStream, File targetDirectory) throws IOException {
    ZipEntry entry;
    while ((entry = jarInputStream.getNextEntry()) != null) {
      File output = new File(targetDirectory, entry.getName());

      if (entry.isDirectory()) {
        output.mkdirs();
      } else {
        output.getParentFile().mkdirs();
        ByteStreams.copy(jarInputStream, Files.newOutputStreamSupplier(output));
      }
    }
  }

  private BundleJarUtil() {
  }
}
