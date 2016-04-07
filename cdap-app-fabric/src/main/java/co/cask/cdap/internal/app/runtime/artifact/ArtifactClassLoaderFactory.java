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

import co.cask.cdap.app.runtime.ProgramRuntimeProvider;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.FilterClassLoader;
import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.runtime.ProgramRuntimeProviderLoader;
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
final class ArtifactClassLoaderFactory {
  private static final Logger LOG = LoggerFactory.getLogger(ArtifactClassLoaderFactory.class);

  private final CConfiguration cConf;
  private final ProgramRuntimeProviderLoader runtimeProviderLoader;
  private final File tmpDir;

  ArtifactClassLoaderFactory(CConfiguration cConf, ProgramRuntimeProviderLoader runtimeProviderLoader) {
    this.cConf = cConf;
    this.runtimeProviderLoader = runtimeProviderLoader;
    this.tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
  }

  /**
   * Create a classloader that uses the artifact at the specified location to load classes, with access to
   * packages that all program type has access to. The classloader created is only for artifact inspection purpose
   * and shouldn't be used for program execution as it doesn't have the proper class filtering for the specific
   * program type for the program being executed.
   *
   * @param artifactLocation the location of the artifact to create the classloader from
   * @return a closeable classloader based off the specified artifact; on closing the returned {@link ClassLoader},
   *         all temporary resources created for the classloader will be removed
   * @throws IOException if there was an error copying or unpacking the artifact
   */
  CloseableClassLoader createClassLoader(Location artifactLocation) throws IOException {
    final File unpackDir = BundleJarUtil.unJar(artifactLocation, DirUtils.createTempDir(tmpDir));

    ClassLoader parentClassLoader;

    ProgramRuntimeProvider sparkRuntimeProvider = runtimeProviderLoader.get(ProgramType.SPARK);
    if (sparkRuntimeProvider != null) {
      // Always have spark classes visible for artifact class loading purpose since we don't know if
      // any classes inside the artifact is a Spark program
      parentClassLoader = new FilterClassLoader(sparkRuntimeProvider.getClass().getClassLoader(),
                                                sparkRuntimeProvider.createProgramClassLoaderFilter(ProgramType.SPARK));
    } else {
      // If no Spark is available, just use the standard filter
      parentClassLoader = new FilterClassLoader(getClass().getClassLoader(), FilterClassLoader.defaultFilter());
    }

    final ProgramClassLoader programClassLoader = new ProgramClassLoader(cConf, unpackDir, parentClassLoader);
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
