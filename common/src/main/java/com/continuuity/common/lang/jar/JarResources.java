/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.common.lang.jar;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
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
public final class JarResources {
  private static final Logger LOG = LoggerFactory.getLogger(JarResources.class);

  // archive resource mapping tables
  private final Map<String, byte[]> classContents = Maps.newHashMap();

  // Set of all non-directory entry names
  private final Set<String> fileEntries = Sets.newHashSet();
  private final Manifest manifest;
  private final Location jarLocation;

  /**
   * Creates a JarResources using a {@link Location}. It extracts all resources from
   * a Jar into a internal map, keyed by resource names.
   *
   * @param jar location of JAR file.
   * @throws IOException
   */
  public JarResources(Location jar) throws IOException {
    this.jarLocation = jar;
    manifest = init(jar);
  }

  /**
   * Returns the {@link java.util.jar.Manifest} object if it presents in the archive file, or {@code null} otherwise.
   *
   * @see JarFile#getManifest()
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
   * Returns an {@link InputStream} for resource with the given name.
   * @param name Name of the resource.
   * @return An opened {@link InputStream} or {@code null} if no such resource exists. Caller is responsible for
   *         closing the stream.
   * @throws IOException
   */
  public InputStream getResourceAsStream(String name) throws IOException {
    if (classContents.containsKey(name)) {
      return new ByteArrayInputStream(classContents.get(name));
    }

    if (!fileEntries.contains(name)) {
      return null;
    }

    // Find the entry that match the given name
    JarInputStream jarInput = new JarInputStream(new BufferedInputStream(jarLocation.getInputStream()));
    JarEntry entry = jarInput.getNextJarEntry();
    while (entry != null && (entry.isDirectory() || !entry.getName().equals(name))) {
      entry = jarInput.getNextJarEntry();
    }

    // The while should found the entry, as the check on fileEntries guarantee it.
    return jarInput;
  }

  /**
   * initializes internal hash tables with Jar file resources.
   */
  private Manifest init(Location jarLocation) throws IOException {
    JarInputStream jarInput = new JarInputStream(new BufferedInputStream(jarLocation.getInputStream()));
    try {
      Manifest manifest = jarInput.getManifest();
      JarEntry ze;
      // For each ".class" entry in the jar file, read the bytes and stores it in the classContents map.
      while ((ze = jarInput.getNextJarEntry()) != null) {
        if (ze.isDirectory()) {
          continue;
        }

        fileEntries.add(ze.getName());

        // The JarInputStream only tries to read the MANIFEST file if it is the first entry in the jar
        // Otherwise, we'll see the manifest file here, hence need to construct it from the current entry.
        if (ze.getName().equals(JarFile.MANIFEST_NAME)) {
          manifest = new Manifest(jarInput);
          continue;
        }

        // Store only the .class files content
        if (!ze.getName().endsWith(".class")) {
          continue;
        }

        // ".class" file would be read in memory, and it shouldn't be too big.
        if (ze.getSize() > Integer.MAX_VALUE) {
          throw new IOException("Jar entry is too big to fit in memory.");
        }

        byte[] bytes;
        if (ze.getSize() < 0) {
          bytes = ByteStreams.toByteArray(jarInput);
        } else {
          bytes = new byte[(int) ze.getSize()];
          ByteStreams.readFully(jarInput, bytes);
        }
        // add to internal resource hashtable
        classContents.put(ze.getName(), bytes);
        if (LOG.isTraceEnabled()) {
          LOG.trace(ze.getName() + "size=" + ze.getSize() + ",csize=" + ze.getCompressedSize());
        }
      }
      return manifest;

    } finally {
      jarInput.close();
    }
  }
}
