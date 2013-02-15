/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.archive;

import com.continuuity.filesystem.Location;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.io.ByteStreams;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;

/**
 * ArchiveBundler is a utlity class that allows to clone a JAR file with modifications
 * to the manifest file and also adds few additional files to the cloned JAR.
 */
public final class ArchiveBundler {
  /**
   * Buffer size of bytes to be read from the input archive into the output archive.
   */
  private static final int BUFFER_SIZE = 10 * 1024 * 1024;

  /**
   * Default location of additional files in cloned archive.
   */
  private static final String DEFAULT_LOC = "META-INF/specification/";

  /**
   * Main archive that needs to be cloned.
   */
  private final Location archive;

  /**
   * Base location where the files in cloned archive will be stored.
   */
  private final String jarEntryPrefix;


  /**
   * Constructor with archive to be cloned.
   * @param archive to be cloned.
   */
  public ArchiveBundler(Location archive) {
    this(archive, "META-INF/specification/");
  }

  /**
   * Constructor that takes in the archive to be cloned
   * @param archive to be cloned
   * @param jarEntryPrefix within cloned archive for additional files.
   */
  public ArchiveBundler(Location archive, String jarEntryPrefix) {
    Preconditions.checkNotNull(jarEntryPrefix, "Entry prefix of additional files in JAR is null");
    this.archive = archive;
    if(jarEntryPrefix.charAt(jarEntryPrefix.length() - 1) != '/') {
      jarEntryPrefix += "/";
    }
    this.jarEntryPrefix = jarEntryPrefix;
  }

  /**
   * Clones the input <code>archive</code> file with MANIFEST file and also adds addition
   * Files to the cloned archive.
   *
   * @param output Cloned output archive
   * @param manifest New manifest file to be added to the cloned archive
   * @param files Additional files to be added to cloned archive
   * @throws IOException thrown when issue with handling of files.
   */
  public void clone(Location output, final Manifest manifest, Location[] files) throws IOException {
    clone(output, manifest, files, new Predicate<ZipEntry>() {
      @Override
      public boolean apply(@Nullable ZipEntry input) {
        return false;
      }
    });
  }

  /**
   * Clones the input <code>archive</code> file with MANIFEST file and also adds addition
   * Files to the cloned archive.
   *
   * @param output Cloned output archive
   * @param manifest New manifest file to be added to the cloned archive
   * @param files Additional files to be added to cloned archive
   * @param filter Filter applied on ZipEntry, if true file is filtered else kept in output.
   * @throws IOException thrown when issue with handling of files.
   */
  public void clone(Location output, final Manifest manifest, Location[] files, Predicate<ZipEntry> filter)
    throws IOException {
    Preconditions.checkNotNull(manifest, "Null manifest");
    Preconditions.checkNotNull(files);

    byte[] buf = new byte[BUFFER_SIZE];

    // Create a input stream based on the original archive file.
    JarInputStream zin = new JarInputStream(archive.getInputStream());

    // Create a new output JAR file with new MANIFEST.
    JarOutputStream zout = new JarOutputStream(output.getOutputStream(), manifest);

    try {
      try {
        // Iterates through the input zip entry and make sure, the new files
        // being added are not already present. If not, they are added to the
        // output zip.
        ZipEntry entry = zin.getNextEntry();
        while (entry != null) {
          String name = entry.getName();

          // Invoke the predicate to see if the entry needs to be filtered.
          // If the filter returns true, then it needs to be filtered; false keep it.
          if(filter.apply(entry)) {
            entry = zin.getNextEntry();
            continue;
          }

          boolean absenceInJar = true;
          for (Location f : files) {
            if (f.getName().equals(name)) {
              absenceInJar = false;
              break;
            }
          }

          if (absenceInJar) {
            zout.putNextEntry(new ZipEntry(name));
            ByteStreams.copy(zin, zout);
          }
          entry = zin.getNextEntry();
        }
      } finally {
        // Close the stream
        if(zin != null) {
          zin.close();
        }
      }

      // Add the new files.
      for (int i = 0; i < files.length; i++) {
        InputStream in = null;
        try {
          in = files[i].getInputStream();
          zout.putNextEntry(new ZipEntry(jarEntryPrefix + files[i].getName()));
          ByteStreams.copy(in, zout);
          zout.closeEntry();
        } finally {
          if(in != null) {
            in.close();
          }
        }
      }
    } finally {
      if(zout != null) {
        zout.close();
      }
    }
  }
}
