/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.common.lang.jar;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

/**
 * JarResources: JarResources maps all resources included in a
 * Zip or Jar file. Additionaly, it provides a method to extract one
 * as a blob.
 */
public final class ProgramJarResources {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramJarResources.class);

  // archive resource mapping tables
  private final Map<String, byte[]> classContents = Maps.newHashMap();

  // Set of all non-directory entry names
  private final Set<String> fileEntries = Sets.newHashSet();
  private final Manifest manifest;
  private final File unpackedJarDir;

  /**
   * Creates a JarResources using a {@link org.apache.twill.filesystem.Location}. It extracts all resources from
   * a Jar into a internal map, keyed by resource names.
   *
   * @param unpackedJarDir directory of the unpacked Jar.
   * @throws java.io.IOException
   */
  public ProgramJarResources(File unpackedJarDir) throws IOException {
    Preconditions.checkArgument(unpackedJarDir != null);
    Preconditions.checkArgument(unpackedJarDir.exists());
    Preconditions.checkArgument(unpackedJarDir.canRead());
    Preconditions.checkArgument(unpackedJarDir.isDirectory());
    this.unpackedJarDir = unpackedJarDir;
    manifest = init(unpackedJarDir);
  }

  /**
   * Returns the {@link java.util.jar.Manifest} object if it presents in the archive file, or {@code null} otherwise.
   *
   * @see java.util.jar.JarFile#getManifest()
   */
  public Manifest getManifest() {
    return manifest;
  }


  /**
   * Checks if an entry exists with the given name.
   * @param name Name of the entry to check
   * @return {@code true} if the entry exists, {@code false} otherwise.
   */
  public boolean contains(String name) {
    return fileEntries.contains(name);
  }

  /**
   * Extracts a archive resource as a blob.
   *
   * @param name a resource name.
   * @return A byte array containing content of the given name or {@code null} if not such entry exists.
   */
  public byte[] getResource(String name) {
    if (classContents.containsKey(name)) {
      return classContents.get(name);
    }

    // Read fully from resource stream and return the bytes.
    try {
      InputStream input = getResourceAsStream(name);
      if (input == null) {
        return null;
      }
      try {
        return ByteStreams.toByteArray(input);
      } finally {
        input.close();
      }

    } catch (IOException e) {
      return null;
    }
  }

  /**
   * Returns an {@link java.io.InputStream} for resource with the given name.
   * @param name Name of the resource.
   * @return An opened {@link java.io.InputStream} or {@code null} if no such resource exists. Caller is responsible for
   *         closing the stream.
   * @throws java.io.IOException
   */
  public InputStream getResourceAsStream(String name) throws IOException {
    if (classContents.containsKey(name)) {
      return new ByteArrayInputStream(classContents.get(name));
    }

    if (!fileEntries.contains(name)) {
      return null;
    }

    return new FileInputStream(new File(unpackedJarDir, name));
  }

  /**
   * initializes internal hash tables with Jar file resources.
   * @param unpackedJarDir
   */
  private Manifest init(File unpackedJarDir) throws IOException {
    Preconditions.checkArgument(unpackedJarDir != null);
    Preconditions.checkArgument(unpackedJarDir.exists());
    Preconditions.checkArgument(unpackedJarDir.canRead());
    Preconditions.checkArgument(unpackedJarDir.isDirectory());

    Manifest manifest = null;
    Queue<File> fileQueue = new LinkedList<File>();
    fileQueue.add(unpackedJarDir);

    UnpackedJarDirIterator jarDirIterator = new UnpackedJarDirIterator(unpackedJarDir);

    while (jarDirIterator.hasMoreElements()) {
      UnpackedJarDirEntry ze = jarDirIterator.nextElement();

      InputStream entryInputStream = ze.getInputStream();
      String relativeFileName = ze.getName();
      long fileSize = ze.getSize();

      fileEntries.add(relativeFileName);

      // The JarInputStream only tries to read the MANIFEST file if it is the first entry in the jar
      // Otherwise, we'll see the manifest file here, hence need to construct it from the current entry.
      if (relativeFileName.equals(JarFile.MANIFEST_NAME)) {
        manifest = new Manifest(entryInputStream);
        continue;
      }

      // Store only the .class files content
      if (!relativeFileName.endsWith(".class")) {
        continue;
      }

      // ".class" file would be read in memory, and it shouldn't be too big.
      // TODO: better max filesize checking / handling
      if (fileSize > Integer.MAX_VALUE) {
        throw new IOException("Unpacked jar entry is too big to fit in memory: " + relativeFileName);
      }

      byte[] bytes;
      if (fileSize < 0) {
        bytes = ByteStreams.toByteArray(entryInputStream);
      } else {
        bytes = new byte[(int) fileSize];
        ByteStreams.readFully(entryInputStream, bytes);
      }

      // add to internal resource hashtable
      classContents.put(ze.getName(), bytes);
      if (LOG.isTraceEnabled()) {
        LOG.trace(ze.getName() + " size=" + fileSize);
      }
    }

    return manifest;
  }
}
