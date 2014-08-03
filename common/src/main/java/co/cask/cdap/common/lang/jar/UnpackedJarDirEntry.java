/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.common.lang.jar;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

/**
 *
 */
public class UnpackedJarDirEntry {

  private final File unpackedJarDir;
  private final File file;

  public UnpackedJarDirEntry(File unpackedJarDir, File file) {
    this.unpackedJarDir = unpackedJarDir;
    this.file = file;
  }

  public boolean isDirectory() {
    return file.isDirectory();
  }

  public long getSize() {
    return file.length();
  }

  public InputStream getInputStream() {
    try {
      return new FileInputStream(file);
    } catch (FileNotFoundException e) {
      return null;
    }
  }

  public File getFile() {
    return file;
  }

  public String getName() {
    return unpackedJarDir.toURI().relativize(file.toURI()).getPath();
  }
}
