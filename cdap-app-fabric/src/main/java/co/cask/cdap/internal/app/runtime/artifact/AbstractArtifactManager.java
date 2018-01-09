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

package co.cask.cdap.internal.app.runtime.artifact;

import co.cask.cdap.api.artifact.ArtifactInfo;
import co.cask.cdap.api.artifact.ArtifactManager;
import co.cask.cdap.api.artifact.CloseableClassLoader;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.DirectoryClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import com.google.common.io.Closeables;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG = LoggerFactory.getLogger(AbstractArtifactManager.class);

  private final File tmpDir;
  private final ClassLoader bootstrapClassLoader;

  protected AbstractArtifactManager(CConfiguration cConf) {
    File tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    this.tmpDir = DirUtils.createTempDir(tmpDir);
    this.bootstrapClassLoader = new URLClassLoader(new URL[0], null);
  }

  /**
   * Returns the {@link Location} of the give artifact.
   *
   * @param artifactInfo information of the artifact
   * @return the {@link Location} of the artifact
   * @throws IOException if failed to locate the {@link Location} of the artifact
   */
  protected abstract Location getArtifactLocation(ArtifactInfo artifactInfo) throws IOException;

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
                                                @Nullable ClassLoader parentClassLoader) throws IOException {
    File unpackedDir = DirUtils.createTempDir(tmpDir);
    BundleJarUtil.unJar(getArtifactLocation(artifactInfo), unpackedDir);
    DirectoryClassLoader directoryClassLoader =
      new DirectoryClassLoader(unpackedDir,
                               parentClassLoader == null ? bootstrapClassLoader : parentClassLoader, "lib");
    return new CloseableClassLoader(directoryClassLoader, new ClassLoaderCleanup(directoryClassLoader, unpackedDir));
  }

  /**
   * Helper class to cleanup temporary directory created for artifact classloader.
   */
  private static final class ClassLoaderCleanup implements Closeable {
    private final File directory;
    private final DirectoryClassLoader directoryClassLoader;

    private ClassLoaderCleanup(DirectoryClassLoader directoryClassLoader, File directory) {
      this.directoryClassLoader = directoryClassLoader;
      this.directory = directory;
    }

    @Override
    public void close() throws IOException {
      try {
        Closeables.closeQuietly(directoryClassLoader);
        DirUtils.deleteDirectoryContents(directory);
      } catch (IOException e) {
        LOG.warn("Failed to delete directory {}", directory, e);
      }
    }
  }
}
