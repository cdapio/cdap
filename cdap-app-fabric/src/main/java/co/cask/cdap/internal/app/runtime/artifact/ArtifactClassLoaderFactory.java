/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

import co.cask.cdap.app.runtime.ProgramClassLoaderProvider;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.FilterClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.runtime.ProgramClassLoader;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.security.impersonation.EntityImpersonator;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Given an artifact, creates a {@link CloseableClassLoader} from it. Takes care of unpacking the artifact and
 * cleaning up the directory when the classloader is closed.
 */
final class ArtifactClassLoaderFactory {
  private static final Logger LOG = LoggerFactory.getLogger(ArtifactClassLoaderFactory.class);

  private final CConfiguration cConf;
  private final ProgramRunnerFactory programRunnerFactory;
  private final File tmpDir;

  ArtifactClassLoaderFactory(CConfiguration cConf, ProgramRunnerFactory programRunnerFactory) {
    this.cConf = cConf;
    this.programRunnerFactory = programRunnerFactory;
    this.tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
  }

  /**
   * Create a classloader that loads classes from a directory where an artifact jar has been expanded, with access to
   * packages that all program type has access to. The classloader created is only for artifact inspection purpose
   * and shouldn't be used for program execution as it doesn't have the proper class filtering for the specific
   * program type for the program being executed.
   *
   * @param unpackDir the directory where the artifact jar has been expanded
   * @return a closeable classloader based off the specified artifact; on closing the returned {@link ClassLoader},
   *         all temporary resources created for the classloader will be removed
   * @throws IOException if there was an error copying or unpacking the artifact
   */
  CloseableClassLoader createClassLoader(File unpackDir) throws IOException {
    ProgramRunner programRunner = null;
    try {
      // Try to create a ProgramClassLoader from the Spark runtime system if it is available.
      // It is needed because we don't know what program types that an artifact might have.
      // TODO: CDAP-5613. We shouldn't always expose the Spark classes.
      programRunner = programRunnerFactory.create(ProgramType.SPARK);
    } catch (Exception e) {
      // If Spark is not supported, exception is expected. We'll use the default filter.
      LOG.trace("Spark is not supported. Not using ProgramClassLoader from Spark", e);
    }

    ProgramClassLoader programClassLoader;
    if (programRunner instanceof ProgramClassLoaderProvider) {
      programClassLoader = new ProgramClassLoader(
        cConf, unpackDir, ((ProgramClassLoaderProvider) programRunner).createProgramClassLoaderParent());
    } else {
      programClassLoader = new ProgramClassLoader(cConf, unpackDir,
                                                  FilterClassLoader.create(getClass().getClassLoader()));
    }

    final ClassLoader finalProgramClassLoader = programClassLoader;
    final ProgramRunner finalProgramRunner = programRunner;
    return new CloseableClassLoader(programClassLoader, new Closeable() {
      @Override
      public void close() {
        Closeables.closeQuietly((Closeable) finalProgramClassLoader);
        if (finalProgramRunner instanceof Closeable) {
          Closeables.closeQuietly((Closeable) finalProgramRunner);
        }
      }
    });
  }

  /**
   * Unpack the given {@code artifactLocation} to a temporary directory and call
   * {@link #createClassLoader(File)} to create the {@link ClassLoader}.
   *
   * @param artifactLocation the location of the artifact to create the classloader from
   * @return a closeable classloader based off the specified artifact; on closing the returned {@link ClassLoader},
   *         all temporary resources created for the classloader will be removed
   * @throws IOException if there was an error copying or unpacking the artifact
   * @see #createClassLoader(File)
   */
  CloseableClassLoader createClassLoader(final Location artifactLocation,
                                         EntityImpersonator entityImpersonator) throws IOException {
    try {
      final File unpackDir = entityImpersonator.impersonate(new Callable<File>() {
        @Override
        public File call() throws IOException {
          return BundleJarUtil.unJar(artifactLocation, DirUtils.createTempDir(tmpDir));
        }
      });

      final CloseableClassLoader classLoader = createClassLoader(unpackDir);
      return new CloseableClassLoader(classLoader, new Closeable() {
        @Override
        public void close() throws IOException {
          try {
            Closeables.closeQuietly(classLoader);
            DirUtils.deleteDirectoryContents(unpackDir);
          } catch (IOException e) {
            LOG.warn("Failed to delete directory {}", unpackDir, e);
          }
        }
      });
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
