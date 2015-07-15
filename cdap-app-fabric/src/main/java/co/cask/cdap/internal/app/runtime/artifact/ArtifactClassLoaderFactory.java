/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.artifact;

import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import com.google.common.io.Closeables;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * Given an artifact, creates a {@link CloseableClassLoader} from it. Takes care of unpacking the artifact and
 * cleaning up the directory when the classloader is closed.
 */
class ArtifactClassLoaderFactory {
  private static final Logger LOG = LoggerFactory.getLogger(ArtifactClassLoaderFactory.class);
  private final File baseUnpackDir;

  ArtifactClassLoaderFactory(File baseUnpackDir) {
    this.baseUnpackDir = baseUnpackDir;
  }

  CloseableClassLoader createClassLoader(Location artifactLocation) throws IOException {
    final File unpackDir = DirUtils.createTempDir(baseUnpackDir);
    BundleJarUtil.unpackProgramJar(artifactLocation, unpackDir);
    final ProgramClassLoader programClassLoader = ProgramClassLoader.create(unpackDir, getClass().getClassLoader());
    return new CloseableClassLoader(programClassLoader, new Closeable() {
      @Override
      public void close() {
        try {
          Closeables.closeQuietly(programClassLoader);
          DirUtils.deleteDirectoryContents(unpackDir);
        } catch (IOException e) {
          LOG.warn("Failed to delete directory {}", unpackDir, e);
        }
      }
    });
  }
}
