/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.archive;

import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
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
   * creates a JarResources. It extracts all resources from a Jar
   * into an internal hashtable, keyed by resource names.
   * @param jarFileName a archive or zip file
   */
  public JarResources(String jarFileName) throws JarResourceException {
    manifest = init(jarFileName);
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
   * @param name a resource name.
   */
  public byte[] getResource(String name) {
    return entryContents.get(name);
  }

  /**
   * initializes internal hash tables with Jar file resources.
   */
  private Manifest init(String jarFileName) throws JarResourceException {
    try {
      // extracts just sizes only.
      JarFile zf = new JarFile(jarFileName);
      try {
        Enumeration<JarEntry> entries = zf.entries();

        while (entries.hasMoreElements()) {
          JarEntry ze = entries.nextElement();
          if (LOG.isTraceEnabled()) {
            LOG.trace(dumpJarEntry(ze));
          }

          if (ze.isDirectory()) {
            continue;
          }
          if (ze.getSize() > Integer.MAX_VALUE) {
            throw new JarResourceException("Jar entry is too big to fit in memory.");
          }

          InputStream is = zf.getInputStream(ze);
          try {
            byte[] bytes;
            if (ze.getSize() < 0) {
              bytes = ByteStreams.toByteArray(is);
            } else {
              bytes = new byte[(int)ze.getSize()];
              ByteStreams.readFully(is, bytes);
            }
            // add to internal resource hashtable
            entryContents.put(ze.getName(), bytes);
            LOG.trace(ze.getName() + "size=" + ze.getSize() + ",csize="
                        + ze.getCompressedSize());

          } finally {
            is.close();
          }
        }
        return zf.getManifest();

      } finally {
        zf.close();
      }
    } catch (NullPointerException e){
      LOG.warn("Error during initialization resource. Reason {}", e.getMessage());
      throw new JarResourceException("Null pointer while loading archive file " + jarFileName);
    } catch (FileNotFoundException e) {
      LOG.warn("File {} not found. Reason : {}", jarFileName, e.getMessage());
      throw new JarResourceException("Jar file " + jarFileName + " requested to be loaded is not found");
    } catch (IOException e) {
      LOG.warn("Error while reading file {}. Reason : {}", jarFileName, e.getMessage());
      throw new JarResourceException("Error reading file " + jarFileName + ".");
    }
  }

  /**
   * Dumps a zip entry into a string.
   * @param ze a JarEntry
   */
  private String dumpJarEntry(JarEntry ze) {
    StringBuilder sb=new StringBuilder();
    if (ze.isDirectory()) {
      sb.append("d ");
    } else {
      sb.append("f ");
    }

    if (ze.getMethod()==JarEntry.STORED) {
      sb.append("stored   ");
    } else {
      sb.append("defalted ");
    }

    sb.append(ze.getName()).append("\t").append(ze.getSize());
    if (ze.getMethod()==JarEntry.DEFLATED) {
      sb.append("/").append(ze.getCompressedSize());
    }

    return (sb.toString());
  }
}
