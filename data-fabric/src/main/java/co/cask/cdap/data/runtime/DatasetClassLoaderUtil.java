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

package co.cask.cdap.data.runtime;

import co.cask.cdap.common.utils.DirUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Utility class to store the {@link java.lang.ClassLoader} and the expanded jar files. It provides functions to get
 * classLoader and cleanup of the jar files.
 */
public class DatasetClassLoaderUtil {
  ClassLoader classLoader;
  Set<File> datasetTempFiles;

  public DatasetClassLoaderUtil(ClassLoader classLoader, Set<File> datasetTempFiles) {
    this.classLoader = classLoader;
    this.datasetTempFiles = datasetTempFiles;
  }
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  public void cleanup() throws IOException {
    for (File file : datasetTempFiles) {
      DirUtils.deleteDirectoryContents(file);
    }
  }
}
