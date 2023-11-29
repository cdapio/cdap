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

package io.cdap.cdap.internal.app.runtime.artifact;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.DirectoryClassLoader;
import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.lang.jar.ClassLoaderFolder;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.runtime.ProgramClassLoader;
import io.cdap.cdap.internal.app.runtime.ProgramRuntimeProviderLoader;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import java.io.Closeable;
import java.io.File;
import java.util.Iterator;
import javax.annotation.Nullable;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Given an artifact, creates a {@link CloseableClassLoader} from it. Takes care of unpacking the
 * artifact and cleaning up the directory when the classloader is closed.
 */
final class ArtifactClassLoaderFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ArtifactClassLoaderFactory.class);

  private final CConfiguration cConf;
  @Nullable
  private final ProgramRuntimeProviderLoader programRuntimeProviderLoader;
  private final File tmpDir;

  @VisibleForTesting
  ArtifactClassLoaderFactory(CConfiguration cConf) {
    this(cConf, null);
  }

  ArtifactClassLoaderFactory(CConfiguration cConf,
      @Nullable ProgramRuntimeProviderLoader programRuntimeProviderLoader) {
    this.cConf = cConf;
    this.programRuntimeProviderLoader = programRuntimeProviderLoader;
    this.tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
        cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
  }

  /**
   * Create a classloader that loads classes from a directory where an artifact jar has been
   * expanded, with access to packages that all program type has access to. The classloader created
   * is only for artifact inspection purpose and shouldn't be used for program execution as it
   * doesn't have the proper class filtering for the specific program type for the program being
   * executed.
   *
   * @param unpackDir the directory where the artifact jar has been expanded
   * @return a closeable classloader based off the specified artifact; on closing the returned
   *     {@link ClassLoader}, all temporary resources created for the classloader will be removed
   */
  CloseableClassLoader createClassLoader(File unpackDir) {
    ClassLoader sparkClassLoader = null;
    if (programRuntimeProviderLoader != null) {
      try {
        // Try to create a ProgramClassLoader from the Spark runtime system if it is available.
        // It is needed because we don't know what program types that an artifact might have.
        // TODO: CDAP-5613. We shouldn't always expose the Spark classes.
        sparkClassLoader = programRuntimeProviderLoader.get(ProgramType.SPARK)
            .createProgramClassLoader(cConf, ProgramType.SPARK);
      } catch (Exception e) {
        // If Spark is not supported, exception is expected. We'll use the default filter.
        LOG.warn("Spark is not supported. Not using ProgramClassLoader from Spark");
        LOG.trace("Failed to create spark program runner with error:", e);
      }
    }

    ProgramClassLoader programClassLoader = null;
    if (sparkClassLoader != null) {
      programClassLoader = new ProgramClassLoader(cConf, unpackDir, sparkClassLoader);
    } else {
      programClassLoader = new ProgramClassLoader(cConf, unpackDir,
          FilterClassLoader.create(getClass().getClassLoader()));
    }

    final ClassLoader finalProgramClassLoader = programClassLoader;
    ClassLoader finalSparkClassLoader = sparkClassLoader;
    return new CloseableClassLoader(programClassLoader, () -> {
      if (finalProgramClassLoader instanceof Closeable) {
        Closeables.closeQuietly((Closeable) finalProgramClassLoader);
      }
      if (finalSparkClassLoader instanceof Closeable) {
        Closeables.closeQuietly((Closeable) finalSparkClassLoader);
      }
    });
  }

  /**
   * Unpack the given {@code artifactLocation} to a temporary directory and call {@link
   * #createClassLoader(File)} to create the {@link ClassLoader}.
   *
   * @param artifactLocation the location of the artifact to create the classloader from
   * @return a closeable classloader based off the specified artifact; on closing the returned
   *     {@link ClassLoader}, all temporary resources created for the classloader will be removed
   * @see #createClassLoader(File)
   */
  CloseableClassLoader createClassLoader(Location artifactLocation,
      EntityImpersonator entityImpersonator) {
    try {
      ClassLoaderFolder classLoaderFolder = entityImpersonator.impersonate(
          () -> BundleJarUtil.prepareClassLoaderFolder(artifactLocation,
              () -> DirUtils.createTempDir(tmpDir)));

      CloseableClassLoader classLoader = createClassLoader(classLoaderFolder.getDir());
      return new CloseableClassLoader(classLoader, () -> {
        Closeables.closeQuietly(classLoader);
        Closeables.closeQuietly(classLoaderFolder);
      });
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Creates a multi level classloader where each location in the specified iterator corresponds to
   * a classloader whose parent is built from the location behind it.
   *
   * @param artifactLocations the locations of the artifact to create the classloader from
   * @return a closeable classloader based off the specified artifacts; on closing the returned
   *     {@link ClassLoader}, all temporary resources created for the classloader will be removed
   * @see #createClassLoader(File)
   */
  CloseableClassLoader createClassLoader(Iterator<Location> artifactLocations,
      EntityImpersonator entityImpersonator) {
    if (!artifactLocations.hasNext()) {
      throw new IllegalArgumentException("Cannot create a classloader without an artifact.");
    }

    Location artifactLocation = artifactLocations.next();
    if (!artifactLocations.hasNext()) {
      return createClassLoader(artifactLocation, entityImpersonator);
    }

    try {
      ClassLoaderFolder classLoaderFolder = entityImpersonator.impersonate(
          () -> BundleJarUtil.prepareClassLoaderFolder(artifactLocation,
              () -> DirUtils.createTempDir(tmpDir)));

      CloseableClassLoader parentClassLoader = createClassLoader(artifactLocations,
          entityImpersonator);
      return new CloseableClassLoader(new DirectoryClassLoader(classLoaderFolder.getDir(),
          parentClassLoader, "lib"), () -> {
        Closeables.closeQuietly(parentClassLoader);
        Closeables.closeQuietly(classLoaderFolder);
      });
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
