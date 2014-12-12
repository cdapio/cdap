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

package co.cask.cdap.test;

import com.google.common.base.Throwables;

import java.io.File;
import java.io.IOException;

/**
 * Utility for creating temp folder in unit-tests. All files and dirs created using it will be cleaned up by
 * {@link java.io.File#deleteOnExit()} which is set for every created dir and file automatically.
 *
 * Note: it is preferable to use {@link org.junit.rules.TemporaryFolder} when you can instead of this tool. Use this one
 *       when you don't have access to the unit-test test lifecycle (like static init of some utility classes, etc.).
 */
public class TempFolder {
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
      folder.deleteOnExit();
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
      file.deleteOnExit();
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
    file.deleteOnExit();
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
    recursiveDelete(folder);
  }

  private void recursiveDelete(File file) {
    File[] files = file.listFiles();
    if (files != null) {
      for (File each : files) {
        recursiveDelete(each);
      }
    }
    file.delete();
  }
}
