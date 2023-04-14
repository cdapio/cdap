/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package io.cdap.cdap.common.lang.jar;

import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.ThrowingSupplier;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.CopyOption;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.util.function.Predicate;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import javax.annotation.Nullable;
import org.apache.twill.filesystem.Location;

/**
 * Utility functions that operate on bundle jars.
 */
public class BundleJarUtil {

  private static final Predicate<String> NO_FILTER = new Predicate<String>() {
    @Override
    public boolean test(String s) {
      return false;
    }
  };

  /**
   * Gets the {@link Manifest} inside the given jar.
   *
   * @param jarLocation Location of the jar file.
   * @return The manifest inside the jar file or {@code null} if no manifest inside the jar file.
   * @throws IOException if failed to load the manifest.
   */
  @Nullable
  public static Manifest getManifest(Location jarLocation) throws IOException {
    // Small optimization if the location is local
    URI uri = jarLocation.toURI();
    if ("file".equals(uri.getScheme())) {
      return getManifest(new File(uri.getPath()));
    }

    // Otherwise search for the Manifest file
    try (JarInputStream is = new JarInputStream(
        new BufferedInputStream(jarLocation.getInputStream()))) {
      return getManifest(is);
    }
  }

  /**
   * Gets the {@link Manifest} inside the given jar.
   *
   * @param url the {@link URL} of the jar
   * @return The manifest inside the jar file or {@code null} if no manifest inside the jar file.
   * @throws IOException if failed to load the manifest.
   */
  @Nullable
  public static Manifest getManifest(URL url) throws IOException {
    // Small optimization if the location is local
    if ("file".equals(url.getProtocol())) {
      return getManifest(new File(url.getPath()));
    }

    // Otherwise search for the Manifest file
    try (JarInputStream is = new JarInputStream(new BufferedInputStream(url.openStream()))) {
      return getManifest(is);
    }
  }

  /**
   * Gets the {@link Manifest} inside the given jar.
   *
   * @param file the jar file.
   * @return The manifest inside the jar file or {@code null} if no manifest inside the jar file.
   * @throws IOException if failed to load the manifest.
   */
  @Nullable
  public static Manifest getManifest(File file) throws IOException {
    try (JarFile jarFile = new JarFile(file)) {
      return jarFile.getManifest();
    }
  }

  /**
   * Creates an JAR including all the files present in the given input. Same as calling
   * {@link #addToArchive(File, ZipOutputStream)} with a {@link JarOutputStream}.
   */
  public static void createJar(File input, File output) throws IOException {
    try (JarOutputStream jarOut = new JarOutputStream(
        new BufferedOutputStream(new FileOutputStream(output)))) {
      addToArchive(input, jarOut);
    }
  }

  /**
   * Adds file(s) to a zip archive. If the given input file is a directory, all files under it will
   * be added recursively.
   *
   * @param input input directory (or file) whose contents needs to be archived
   * @param output an opened {@link ZipOutputStream} for the archive content to add to
   * @throws IOException if there is failure in the archive creation
   */
  public static void addToArchive(File input, ZipOutputStream output) throws IOException {
    addToArchive(input, false, output);
  }

  /**
   * Adds file(s) to a zip archive. If the given input file is a directory, all files under it will
   * be added recursively.
   *
   * @param input input directory (or file) whose contents needs to be archived
   * @param includeDirName if {@code true} and if the input is a directory, prefix each entries
   *     with the directory name
   * @param output an opened {@link ZipOutputStream} for the archive content to add to
   * @throws IOException if there is failure in the archive creation
   */
  public static void addToArchive(final File input, final boolean includeDirName,
      final ZipOutputStream output) throws IOException {
    addToArchive(input, includeDirName, output, NO_FILTER);
  }

  /**
   * Adds file(s) to a zip archive. If the given input file is a directory, all files under it will
   * be added recursively.
   *
   * @param input input directory (or file) whose contents needs to be archived
   * @param includeDirName if {@code true} and if the input is a directory, prefix each entries
   *     with the directory name
   * @param output an opened {@link ZipOutputStream} for the archive content to add to
   * @param fileNameFilter a filter to ignore adding certain files to the JAR. If the predicate
   *     returns true, the file is excluded from the created JAR.
   * @throws IOException if there is failure in the archive creation
   */
  public static void addToArchive(final File input, final boolean includeDirName,
      final ZipOutputStream output,
      final Predicate<String> fileNameFilter) throws IOException {
    final URI baseURI = input.toURI();
    Files.walkFileTree(input.toPath(), EnumSet.of(FileVisitOption.FOLLOW_LINKS),
        Integer.MAX_VALUE, new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
              throws IOException {
            URI uri = baseURI.relativize(dir.toUri());
            String entryName =
                includeDirName ? input.getName() + "/" + uri.getPath() : uri.getPath();

            if (!entryName.isEmpty()) {
              output.putNextEntry(new ZipEntry(entryName));
              output.closeEntry();
            }
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            if (fileNameFilter.test(file.getFileName().toString())) {
              return FileVisitResult.CONTINUE;
            }
            URI uri = baseURI.relativize(file.toUri());
            if (uri.getPath().isEmpty()) {
              // Only happen if the given "input" is a file.
              output.putNextEntry(new ZipEntry(file.toFile().getName()));
            } else {
              output.putNextEntry(new ZipEntry(
                  includeDirName ? input.getName() + "/" + uri.getPath() : uri.getPath()));
            }
            Files.copy(file, output);
            output.closeEntry();
            return FileVisitResult.CONTINUE;
          }
        });
  }

  /**
   * Takes a jar or a local directory and prepares a folder to be loaded by classloader. If a jar is
   * provided it unpacks a manifest and any nested jars and links original jar into the destination
   * folder, so that it would be picked up by classloader to load any classes or resources.
   *
   * If a directory is provided, it assumes that this directory already contains the unpacked jar
   * contents (ie. this directory was used as the destinationFolder in a previous call to this
   * method). In this case, no unpacking is needed. The {@link ClassLoaderFolder} returned will be
   * pointing at the provided directory.
   *
   * @param jarLocation Location containing the jar file or local directory with already
   *     unpacked jar files
   * @param destinationSupplier Supply the directory to expand into when needed
   * @return a {@link ClassLoaderFolder} containing the directory with the content ready for
   *     classloader creation.
   * @throws IOException If failed to expand the jar
   */
  public static ClassLoaderFolder prepareClassLoaderFolder(Location jarLocation,
      ThrowingSupplier<File, IOException> destinationSupplier)
      throws IOException {
    return new ClassLoaderFolder(jarLocation, destinationSupplier);
  }

  /**
   * Performs the same operation as the
   * {@link #prepareClassLoaderFolder(Location, ThrowingSupplier)} method.
   */
  public static ClassLoaderFolder prepareClassLoaderFolder(File jarFile,
      ThrowingSupplier<File, IOException> destinationSupplier)
      throws IOException {
    return prepareClassLoaderFolder(Locations.toLocation(jarFile), destinationSupplier);
  }

  /**
   * Unpack a jar file in the given location to a directory.
   *
   * @param jarLocation Location containing the jar file
   * @param destinationFolder Directory to expand into
   * @return The {@code destinationFolder}
   * @throws IOException If failed to expand the jar
   */
  public static File unJar(Location jarLocation, File destinationFolder) throws IOException {
    return unJar(jarLocation, destinationFolder, name -> true);
  }

  /**
   * Unpack a jar file in the given location to a directory.
   *
   * @param jarLocation Location containing the jar file
   * @param destinationFolder Directory to expand into
   * @param nameFilter Predicate to select files to unpack
   * @return The {@code destinationFolder}
   * @throws IOException If failed to expand the jar
   */
  public static File unJar(Location jarLocation, File destinationFolder,
      Predicate<String> nameFilter)
      throws IOException {
    try (ZipInputStream zipIn = new ZipInputStream(
        new BufferedInputStream(jarLocation.getInputStream()))) {
      unJar(zipIn, destinationFolder, nameFilter);
    }
    return destinationFolder;
  }

  /**
   * Unpack a jar file to a directory.
   *
   * @param jarFile the jar file to unpack
   * @param destinationFolder Directory to expand into
   * @return The {@code destinationFolder}
   * @throws IOException If failed to expand the jar
   */
  public static File unJar(File jarFile, File destinationFolder) throws IOException {
    return unJar(jarFile, destinationFolder, name -> true);
  }

  /**
   * Unpack a jar file to a directory, overwriting any existing files.
   *
   * @param jarFile the jar file to unpack
   * @param destinationFolder Directory to expand into
   * @return The {@code destinationFolder}
   * @throws IOException If failed to expand the jar
   */
  public static File unJarOverwrite(File jarFile, File destinationFolder) throws IOException {
    return unJar(jarFile, destinationFolder, name -> true, StandardCopyOption.REPLACE_EXISTING);
  }

  /**
   * Unpack a jar file to a directory.
   *
   * @param jarFile the jar file to unpack
   * @param destinationFolder Directory to expand into
   * @param nameFilter Predicate to select files to unpack
   * @param copyOptions Copy options to use when unpacking
   * @return The {@code destinationFolder}
   * @throws IOException If failed to expand the jar
   */
  private static File unJar(File jarFile, File destinationFolder, Predicate<String> nameFilter,
      CopyOption... copyOptions)
      throws IOException {
    try (ZipInputStream zipIn = new ZipInputStream(
        new BufferedInputStream(new FileInputStream(jarFile)))) {
      unJar(zipIn, destinationFolder, nameFilter, copyOptions);
    }
    return destinationFolder;
  }

  /**
   * Search for {@link Manifest} from the given {@link JarInputStream}.
   *
   * @param jarInput the {@link JarInputStream} to look for {@link Manifest}
   * @return The {@link Manifest} or {@code null} if no manifest was found.
   * @throws IOException if failed to read from the given input
   */
  @Nullable
  private static Manifest getManifest(JarInputStream jarInput) throws IOException {
    // This only looks at the first entry, which if is created with jar util, then it'll be there.
    Manifest manifest = jarInput.getManifest();
    if (manifest != null) {
      return manifest;
    }

    // Otherwise, slow path. Need to goes through the entries
    JarEntry jarEntry = jarInput.getNextJarEntry();
    while (jarEntry != null) {
      if (JarFile.MANIFEST_NAME.equals(jarEntry.getName())) {
        return new Manifest(jarInput);
      }
      jarEntry = jarInput.getNextJarEntry();
    }

    return null;
  }

  private static void unJar(ZipInputStream input, File targetDirectory,
      Predicate<String> nameFilter,
      CopyOption... copyOptions)
      throws IOException {
    Path targetPath = targetDirectory.toPath();
    Files.createDirectories(targetPath);

    ZipEntry entry;
    while ((entry = input.getNextEntry()) != null) {
      if (nameFilter.test(entry.getName())) {
        Path output = targetPath.resolve(entry.getName());

        if (entry.isDirectory()) {
          Files.createDirectories(output);
        } else {
          Files.createDirectories(output.getParent());
          Files.copy(input, output, copyOptions);
        }
      }
    }
  }

  private BundleJarUtil() {
  }
}
