/*
 * Copyright © 2021 Cask Data, Inc.
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
import io.cdap.cdap.common.utils.DirUtils;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.jar.JarFile;
import org.apache.twill.filesystem.Location;

/**
 * Represents a directory that is ready for ClassLoader to use. The {@link #close()} method is for
 * cleaning up local directory after finishing the usage of the directory.
 */
public final class ClassLoaderFolder implements Closeable {

  private final File dir;
  private final boolean needDelete;

  ClassLoaderFolder(Location location, ThrowingSupplier<File, IOException> targetDirSupplier)
      throws IOException {
    if ("file".equals(location.toURI().getScheme()) && location.isDirectory()) {
      this.dir = new File(location.toURI());
      this.needDelete = false;
    } else {
      File targetDir = targetDirSupplier.get();
      Files.createDirectories(targetDir.toPath());
      BundleJarUtil.unJar(location, targetDir,
          name -> name.equals(JarFile.MANIFEST_NAME) || name.endsWith(".jar"));

      // Note: We start with space to ensure this file goes first in case resources order is important
      File artifactTempName = File.createTempFile(" artifact", ".jar", targetDir);
      artifactTempName.delete();
      Locations.linkOrCopy(location, artifactTempName);

      this.dir = targetDir;
      this.needDelete = true;
    }
  }

  public File getDir() {
    return dir;
  }

  @Override
  public void close() throws IOException {
    if (needDelete && dir.exists()) {
      DirUtils.deleteDirectoryContents(dir);
    }
  }
}
