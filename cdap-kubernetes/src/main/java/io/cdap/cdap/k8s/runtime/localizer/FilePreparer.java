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

package io.cdap.cdap.k8s.runtime.localizer;

import org.apache.twill.api.LocalFile;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillRunnable;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * Places files in locations that will be eventually be accessible to the {@link FileLocalizer},
 * either locally in the container or through a remote call back to app-fabric.
 * An instance of this class will be used to prepare files for a single Twill run and is not meant to
 * be shared across runs.
 */
public abstract class FilePreparer {

  /**
   * Prepare a LocalFile for a {@link TwillRunnable} so that it can be pulled by the {@link FileLocalizer}.
   *
   * @param runnableName the name of the TwillRunnable that the file belongs to
   * @param localFile the file to prepare
   * @return the prepared file, which can be pulled by the {@link FileLocalizer}.
   */
  public abstract LocalFile prepareFile(String runnableName, LocalFile localFile) throws IOException;

  /**
   * Creates a jar from the files in the config directory and prepares that jar so that it can
   * be fetched by the {@link FileLocalizer}. This method will be called once after all calls to
   * {@link #prepareFile(String, LocalFile)} have been completed.
   *
   * @param configDir directory containing files to localize
   * @return information about how to fetch the runtime config
   */
  public abstract RuntimeConfig prepareRuntimeConfig(Path configDir) throws IOException;

  protected void writeJar(Path configDir, OutputStream outputStream) throws IOException {
    try (
      JarOutputStream jarOutput = new JarOutputStream(outputStream);
      DirectoryStream<Path> stream = Files.newDirectoryStream(configDir)
    ) {
      for (Path path : stream) {
        jarOutput.putNextEntry(new JarEntry(path.getFileName().toString()));
        Files.copy(path, jarOutput);
        jarOutput.closeEntry();
      }
    }
  }
}
