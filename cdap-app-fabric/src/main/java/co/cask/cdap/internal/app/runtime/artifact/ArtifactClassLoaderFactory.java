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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.lang.FilterClassLoader;
import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.runtime.spark.SparkUtils;
import co.cask.cdap.proto.ProgramType;
import com.google.common.io.Closeables;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URLClassLoader;

/**
 * Given an artifact, creates a {@link CloseableClassLoader} from it. Takes care of unpacking the artifact and
 * cleaning up the directory when the classloader is closed.
 */
public class ArtifactClassLoaderFactory {
  private static final Logger LOG = LoggerFactory.getLogger(ArtifactClassLoaderFactory.class);

  private final CConfiguration cConf;
  private final File baseUnpackDir;

  public ArtifactClassLoaderFactory(CConfiguration cConf, File baseUnpackDir) {
    this.cConf = cConf;
    this.baseUnpackDir = baseUnpackDir;
  }

  /**
   * Create a classloader that uses the artifact at the specified location to load classes, with access to
   * packages that the given program type has access to. See {@link FilterClassLoader} for more detail on
   * what program types have access to what packages.
   *
   * @param artifactLocation the location of the artifact to create the classloader from
   * @return a closeable classloader based off the specified artifact
   * @throws IOException if there was an error copying or unpacking the artifact
   */
  public CloseableClassLoader createClassLoader(Location artifactLocation) throws IOException {
    final File unpackDir = DirUtils.createTempDir(baseUnpackDir);
    BundleJarUtil.unpackProgramJar(artifactLocation, unpackDir);

    // Always have spark classes visible for artifact class loading purpose since we don't know if
    // any classes inside the artifact is a Spark program
    final URLClassLoader parentClassLoader = SparkUtils.createSparkFrameworkClassLoader(getClass().getClassLoader());
    final ProgramClassLoader programClassLoader =
      ProgramClassLoader.create(cConf, unpackDir, parentClassLoader, ProgramType.SPARK);
    return new CloseableClassLoader(programClassLoader, new Closeable() {
      @Override
      public void close() {
        try {
          Closeables.closeQuietly(programClassLoader);
          Closeables.closeQuietly(parentClassLoader);
          DirUtils.deleteDirectoryContents(unpackDir);
        } catch (IOException e) {
          LOG.warn("Failed to delete directory {}", unpackDir, e);
        }
      }
    });
  }
}
