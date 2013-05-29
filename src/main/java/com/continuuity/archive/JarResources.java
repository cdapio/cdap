/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.archive;

import com.continuuity.weave.filesystem.Location;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
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
  private final Map<String, byte[]> entryContents = Maps.newHashMap();
  private final Manifest manifest;

  /**
   * Creates a JarResources using a {@link Location}. It extracts all resources from
   * a Jar into a internal map, keyed by resource names.
   *
   * @param jar location of JAR file.
   * @throws IOException
   */
  public JarResources(Location jar) throws IOException {
    manifest = init(jar);
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
   * Extracts a archive resource as a blob.
   *
   * @param name a resource name.
   */
  public byte[] getResource(String name) {
    return entryContents.get(name);
  }

  /**
   * initializes internal hash tables with Jar file resources.
   */
  private Manifest init(Location jarFile) throws IOException {
    JarInputStream jarInput = new JarInputStream(jarFile.getInputStream());
    try {
      Manifest manifest = jarInput.getManifest();
      JarEntry ze;
      // For each entry in the jar file, read the bytes and stores it in the entryContents map.
      while((ze = jarInput.getNextJarEntry()) != null) {
        if(LOG.isTraceEnabled()) {
          LOG.trace(dumpJarEntry(ze));
        }
        if(ze.isDirectory()) {
          continue;
        }
        if(ze.getSize() > Integer.MAX_VALUE) {
          throw new IOException("Jar entry is too big to fit in memory.");
        }
        // The JarInputStream only tries to read the MANIFEST file if it is the first entry in the jar
        // Otherwise, we'll see the manifest file here, hence need to construct it from the current entry.
        if (ze.getName().equals(JarFile.MANIFEST_NAME)) {
          manifest = new Manifest(jarInput);
          continue;
        }

        byte[] bytes;
        if(ze.getSize() < 0) {
          bytes = ByteStreams.toByteArray(jarInput);
        } else {
          bytes = new byte[(int) ze.getSize()];
          ByteStreams.readFully(jarInput, bytes);
        }
        // add to internal resource hashtable
        entryContents.put(ze.getName(), bytes);
        if(LOG.isTraceEnabled()) {
          LOG.trace(ze.getName() + "size=" + ze.getSize() + ",csize=" + ze.getCompressedSize());
        }
      }
      return manifest;

    } finally {
      jarInput.close();
    }
  }

  /**
   * Dumps a zip entry into a string.
   *
   * @param ze a JarEntry
   */
  private String dumpJarEntry(JarEntry ze) {
    StringBuilder sb = new StringBuilder();
    if(ze.isDirectory()) {
      sb.append("d ");
    } else {
      sb.append("f ");
    }

    if(ze.getMethod() == JarEntry.STORED) {
      sb.append("stored   ");
    } else {
      sb.append("defalted ");
    }

    sb.append(ze.getName()).append("\t").append(ze.getSize());
    if(ze.getMethod() == JarEntry.DEFLATED) {
      sb.append("/").append(ze.getCompressedSize());
    }

    return ( sb.toString() );
  }
}
