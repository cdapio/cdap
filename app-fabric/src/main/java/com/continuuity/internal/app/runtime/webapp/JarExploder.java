package com.continuuity.internal.app.runtime.webapp;

import com.google.common.base.Predicate;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Explodes jar.
 */
public class JarExploder {

  public static int explode(File jarFile, File destDir, Predicate<JarEntry> filter) throws IOException {
    int count = 0;
    final JarFile jar = new JarFile(jarFile);

    try {
      if (!destDir.exists() && !destDir.mkdirs()) {
        throw new IOException(String.format("Cannot create destination dir %s", destDir.getAbsolutePath()));
      }

      Enumeration<JarEntry> entries = jar.entries();

      while (entries.hasMoreElements()) {
        final JarEntry entry = entries.nextElement();
        if (!filter.apply(entry)) {
          continue;
        }

        File file = new File(destDir, entry.getName());
        if (entry.isDirectory()) {
          // Create dir
          if (!file.mkdirs()) {
            throw new IOException(String.format("Cannot create dir %s", file.getAbsolutePath()));
          }
          continue;
        }
        File parentFile = file.getParentFile();
        if (!parentFile.isDirectory() && !parentFile.mkdirs()) {
          throw new IOException(String.format("Cannot create dir %s", parentFile));
        }

        // Write file
        Files.copy(new InputSupplier<InputStream>() {
          @Override
          public InputStream getInput() throws IOException {
            return jar.getInputStream(entry);
          }
        }, file);

        ++count;
      }

      return count;

    } finally {
      jar.close();
    }
  }
}
