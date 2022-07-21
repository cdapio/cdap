/*
 * Copyright © 2018 Cask Data, Inc.
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

import com.google.common.io.Closeables;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactManager;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.DirectoryClassLoader;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.lang.jar.ClassLoaderFolder;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import org.apache.twill.filesystem.Location;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import javax.annotation.Nullable;

/**
 * An abstract base for {@link ArtifactManager} implementation. It has logic to construct the artifact classloader.
 */
public abstract class AbstractArtifactManager implements ArtifactManager {

  private final File tmpDir;
  private final ClassLoader bootstrapClassLoader;

  protected AbstractArtifactManager(CConfiguration cConf) {
    File tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    this.tmpDir = DirUtils.createTempDir(tmpDir);
    this.bootstrapClassLoader = new URLClassLoader(new URL[0], null);
  }

  /**
   * Returns the {@link Location} of the give artifact for artifact ClassLoader creation.
   *
   * @param artifactInfo information of the artifact
   * @param namespace artifact namespace, or null if the program namespace should not be used
   * @return the {@link Location} of the artifact
   * @throws IOException if failed to locate the {@link Location} of the artifact
   */
  protected abstract Location getArtifactLocation(ArtifactInfo artifactInfo,
                                                  @Nullable String namespace) throws IOException, UnauthorizedException;

  /**
   * Create a class loader with artifact jar unpacked contents and parent for this classloader is the supplied
   * parentClassLoader, if that parent classloader is null, bootstrap classloader is used as parent.
   * This is a closeable classloader, caller should call close when he is done using it, during close directory
   * cleanup will be performed.
   *
   * @param artifactInfo artifact info whose artifact will be unpacked to create classloader
   * @param parentClassLoader  optional parent classloader, if null bootstrap classloader will be used
   * @return CloseableClassLoader call close on this CloseableClassLoader for cleanup
   * @throws IOException if artifact is not found or there were any error while getting artifact
   */
  @Override
  public CloseableClassLoader createClassLoader(ArtifactInfo artifactInfo,
                                                @Nullable ClassLoader parentClassLoader)
    throws IOException, UnauthorizedException {
    return createClassLoader(null, artifactInfo, parentClassLoader);
  }

  @Override
  public CloseableClassLoader createClassLoader(@Nullable String namespace, ArtifactInfo artifactInfo,
                                                @Nullable ClassLoader parentClassLoader)
    throws IOException, UnauthorizedException {
    ClassLoaderFolder folder = BundleJarUtil.prepareClassLoaderFolder(getArtifactLocation(artifactInfo, namespace),
                                                                      () -> DirUtils.createTempDir(tmpDir));
    DirectoryClassLoader directoryClassLoader =
      new DirectoryClassLoader(folder.getDir(),
                               parentClassLoader == null ? bootstrapClassLoader : parentClassLoader, "lib");
    return new CloseableClassLoader(directoryClassLoader, new ClassLoaderCleanup(directoryClassLoader, folder));
  }

  /**
   * Helper class to cleanup temporary directory created for artifact classloader.
   */
  private static final class ClassLoaderCleanup implements Closeable {
    private final DirectoryClassLoader directoryClassLoader;
    private final ClassLoaderFolder folder;

    private ClassLoaderCleanup(DirectoryClassLoader directoryClassLoader, ClassLoaderFolder folder) {
      this.directoryClassLoader = directoryClassLoader;
      this.folder = folder;
    }

    @Override
    public void close() throws IOException {
      Closeables.closeQuietly(directoryClassLoader);
      Closeables.closeQuietly(folder);
    }
  }
}
