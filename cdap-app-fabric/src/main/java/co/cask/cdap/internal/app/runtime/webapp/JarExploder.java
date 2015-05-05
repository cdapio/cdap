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

package co.cask.cdap.internal.app.runtime.webapp;

import co.cask.cdap.common.utils.DirUtils;
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
      if (!DirUtils.mkdirs(destDir)) {
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
          if (!DirUtils.mkdirs(file)) {
            throw new IOException(String.format("Cannot create dir %s", file.getAbsolutePath()));
          }
          continue;
        }
        File parentFile = file.getParentFile();
        if (!DirUtils.mkdirs(parentFile)) {
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
