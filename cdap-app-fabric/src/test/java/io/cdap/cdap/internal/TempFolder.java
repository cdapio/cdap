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

package io.cdap.cdap.internal;

import com.google.common.base.Throwables;
import io.cdap.cdap.common.utils.DirUtils;
import java.io.File;
import java.io.IOException;
import org.junit.rules.TemporaryFolder;

/**
 * Utility for creating temp folder in unit-tests.
 *
 * Note: it is preferable to use {@link org.junit.rules.TemporaryFolder} when you can instead of this tool. Use this one
 *       when you don't have access to the unit-test test lifecycle (like static init of some utility classes, etc.).
 *
 * @deprecated Don't use this. Use {@link TemporaryFolder} instead
 */
@Deprecated
final class TempFolder {
  private File folder;

  /**
   * Created temp folder.
   */
  public TempFolder() {
    try {
      folder = File.createTempFile("junit", "");
      folder.delete();
      if (!folder.mkdir()) {
        throw new RuntimeException("Could NOT create temp dir at " + folder.getAbsolutePath());
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Returns a new fresh file with the given name under the temporary folder.
   */
  public File newFile(String fileName) throws IOException {
    try {
      File file = new File(folder, fileName);
      if (!file.createNewFile()) {
        throw new RuntimeException("Could NOT create temp file at " + file.getAbsolutePath());
      }
      return file;
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Returns a new fresh folder with the given name under the temporary folder.
   */
  public File newFolder(String folderName) {
    File file = new File(folder, folderName);
    if (!file.mkdir()) {
      throw new RuntimeException("Could NOT create temp dir at " + file.getAbsolutePath());
    }
    return file;
  }

  /**
   * @return the location of this temporary folder.
   */
  public File getRoot() {
    return folder;
  }

  /**
   * Delete all files and folders under the temporary folder.
   * Usually not called directly, since it is automatically done via deleteOnExit().
   */
  public void delete() {
    try {
      if (folder.isDirectory()) {
        DirUtils.deleteDirectoryContents(folder, true);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
