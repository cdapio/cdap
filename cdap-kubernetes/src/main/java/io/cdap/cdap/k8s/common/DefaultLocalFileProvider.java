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

package io.cdap.cdap.k8s.common;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Provides a File object using local files. Creates the folders up to the file if they do not
 * exist. Does NOT create the file prior to returning a writable {@link File} object.
 */
public class DefaultLocalFileProvider implements LocalFileProvider {

  @Override
  public File getWritableFileRef(String path) throws IOException {
    Path parentDirPath = Paths.get(path).getParent();
    if (parentDirPath != null) {
      File parentDir = new File(parentDirPath.toString());
      if (!parentDir.exists() || !parentDir.isDirectory()) {
        if (!parentDir.mkdirs()) {
          throw new IOException(
              String.format("Failed to create parent directories for file '%s'", path));
        }
      }
    }
    return new File(path);
  }
}
