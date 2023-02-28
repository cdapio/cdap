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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.rules.TemporaryFolder;

/**
 * Provides a file in a provided temporary directory for testing.
 * The temporary local file provider will pre-create the writable file reference prior to returning it to the caller.
 */
public class TemporaryLocalFileProvider implements LocalFileProvider {

  private final TemporaryFolder temporaryFolder;

  public TemporaryLocalFileProvider(TemporaryFolder temporaryFolder) {
    this.temporaryFolder = temporaryFolder;
  }

  @Override
  public File getWritableFileRef(String path) throws IOException {
    Path parentDirPath = Paths.get(path).getParent();
    if (parentDirPath != null) {
      File parentDir = new File(temporaryFolder.getRoot().getPath(), parentDirPath.toString());
      if (!parentDir.exists() || !parentDir.isDirectory()) {
        List<String> folders = new ArrayList<>();
        for (Iterator<Path> pathIterator = parentDirPath.iterator(); pathIterator.hasNext();) {
          folders.add(pathIterator.next().toString());
        }
        temporaryFolder.newFolder(folders.toArray(new String[0]));
      }
    }
    return temporaryFolder.newFile(path);
  }
}
