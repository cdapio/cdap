package com.continuuity.jar;

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;

/**
 * JarBundler is a utlity class that allows to clone a JAR file with modifications
 * to the manifest file and also adds few additional files to the cloned JAR.
 */
public final class JarBundler {
  /**
   * Buffer size of bytes to be read from the input jar into the output jar.
   */
  private static final int BUFFER_SIZE = 10 * 1024 * 1024;

  /**
   * Default location of additional files in cloned jar.
   */
  private static final String DEFAULT_LOC = "META-INF/specification/";

  /**
   * Main jar that needs to be cloned.
   */
  private final File jar;

  /**
   * Base location where the files in cloned jar will be stored.
   */
  private final String jarEntryPrefix;


  /**
   * Constructor with jar to be cloned.
   * @param jar to be cloned.
   */
  public JarBundler(File jar) {
    this(jar, "META-INF/specification/");
  }

  /**
   * Constructor that takes in the jar to be cloned
   * @param jar to be cloned
   * @param jarEntryPrefix within cloned jar for additional files.
   */
  public JarBundler(File jar, String jarEntryPrefix) {
    Preconditions.checkArgument(jar.exists(), "JAR file (%s) to be cloned does not exist.", jar.getAbsolutePath());
    Preconditions.checkNotNull(jarEntryPrefix, "Entry prefix of additional files in JAR is null");
    this.jar = jar;
    if(jarEntryPrefix.charAt(jarEntryPrefix.length() - 1) != '/') {
      jarEntryPrefix += "/";
    }
    this.jarEntryPrefix = jarEntryPrefix;
  }

  /**
   * Clones the input <code>jar</code> file with MANIFEST file and also adds addition
   * Files to the cloned jar.
   *
   * @param output Cloned output jar
   * @param manifest New manifest file to be added to the cloned jar
   * @param files Additional files to be added to cloned jar
   * @throws IOException thrown when issue with handling of files.
   */
  public void clone(File output, final Manifest manifest, File[] files) throws IOException {
    Preconditions.checkArgument(output.exists(), "JAR file %s already exists.", output.getAbsolutePath());
    Preconditions.checkNotNull(manifest, "Null manifest");
    Preconditions.checkNotNull(files);

    byte[] buf = new byte[BUFFER_SIZE];

    // Create a input stream based on the original jar file.
    JarInputStream zin = new JarInputStream(new FileInputStream(jar));

    // Create a new output JAR file with new MANIFEST.
    JarOutputStream zout = new JarOutputStream(new FileOutputStream(output), manifest);

    try {
      try {
        // Iterates through the input zip entry and make sure, the new files
        // being added are not already present. If not, they are added to the
        // output zip.
        ZipEntry entry = zin.getNextEntry();
        while (entry != null) {
          String name = entry.getName();
          boolean absenceInJar = true;
          for (File f : files) {
            if (f.getName().equals(name)) {
              absenceInJar = false;
              break;
            }
          }

          if (absenceInJar) {
            // Add JAR entry to output stream.
            zout.putNextEntry(new ZipEntry(name));
            // Transfer bytes from the JAR file to the output file
            int len;
            while ((len = zin.read(buf)) > 0) {
              zout.write(buf, 0, len);
            }
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
          in = new FileInputStream(files[i]);
          // Add JAR entry to output stream.
          zout.putNextEntry(new ZipEntry(jarEntryPrefix + files[i].getName()));
          // Transfer bytes from the file to the JAR file
          int len;
          while ((len = in.read(buf)) > 0) {
            zout.write(buf, 0, len);
          }
          // Complete the entry
          zout.closeEntry();
        } finally {
          if(in != null) {
            in.close();
          }
        }
      }
    } finally {
      // Complete the JAR file
      if(zout != null) {
        zout.close();
      }
    }
  }
}
