/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset.type;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.common.lang.DirectoryClassLoader;
import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import javax.annotation.Nullable;

/**
 * Creates a {@link ClassLoader} for a {@link DatasetModuleMeta} by unpacking the dataset jar and creating a
 * {@link DirectoryClassLoader} over the unpacked jar. Classloaders are cached, and unpacked directories are cleaned
 * up when the provider is closed. Note that this means changes to dataset code are not picked up, as the assumption
 * is that this provider is created once at the start of a program run and closed at the end.
 */
public class DirectoryClassLoaderProvider implements DatasetClassLoaderProvider {
  private static final Logger LOG = LoggerFactory.getLogger(DirectoryClassLoaderProvider.class);
  private final ClassLoader parentClassLoader;
  private final LoadingCache<URI, ClassLoader> classLoaders;
  private final LocationFactory locationFactory;
  private final File tmpDir;

  public DirectoryClassLoaderProvider(CConfiguration cConf,
                                      @Nullable ClassLoader parentClassLoader,
                                      LocationFactory locationFactory) {
    this.parentClassLoader = parentClassLoader == null ?
      Objects.firstNonNull(Thread.currentThread().getContextClassLoader(), getClass().getClassLoader()) :
      parentClassLoader;
    this.locationFactory = locationFactory;
    this.classLoaders = CacheBuilder.newBuilder().build(new ClassLoaderCacheLoader());
    File baseDir =
      new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR), cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    this.tmpDir = DirUtils.createTempDir(baseDir);
  }

  @Override
  public ClassLoader get(DatasetModuleMeta moduleMeta) throws IOException {
    URI jarLocation = moduleMeta.getJarLocation();
    return jarLocation == null ? parentClassLoader : classLoaders.getUnchecked(jarLocation);
  }

  @Override
  public ClassLoader getParent() {
    return parentClassLoader;
  }

  @Override
  public void close() throws IOException {
    // Cleanup the ClassLoader cache and the directory used for expanded dataset jars
    classLoaders.invalidateAll();
    try {
      LOG.trace("cleaning unpacked dataset jars from {}.", tmpDir.getAbsolutePath());
      DirUtils.deleteDirectoryContents(tmpDir);
    } catch (IOException e) {
      LOG.warn("Failed to delete directory {}", tmpDir);
    }
  }

  /**
   * A CacheLoader that will copy the jar to a temporary location, unpack it, and create a DirectoryClassLoader.
   */
  private final class ClassLoaderCacheLoader extends CacheLoader<URI, ClassLoader> {

    @Override
    public ClassLoader load(URI jarURI) throws Exception {
      if (jarURI == null) {
        return parentClassLoader;
      }
      Location jarLocation = locationFactory.create(jarURI);
      File unpackedDir = DirUtils.createTempDir(tmpDir);
      BundleJarUtil.unpackProgramJar(jarLocation, unpackedDir);
      LOG.trace("unpacking dataset jar from {} to {}.", jarURI.toString(), unpackedDir.getAbsolutePath());

      return new DirectoryClassLoader(unpackedDir, parentClassLoader);
    }
  }
}
