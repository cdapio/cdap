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
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.io.Closeables;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;

/**
 * Creates a {@link ClassLoader} for a {@link DatasetModuleMeta} by unpacking the dataset jar and creating a
 * {@link DirectoryClassLoader} over the unpacked jar. Classloaders are cached, and unpacked directories are cleaned
 * up when the provider is closed. Note that this means changes to dataset code are not picked up, as the assumption
 * is that this provider is created once at the start of a program run and closed at the end.
 */
public class DirectoryClassLoaderProvider implements DatasetClassLoaderProvider {
  private static final Logger LOG = LoggerFactory.getLogger(DirectoryClassLoaderProvider.class);
  private final LoadingCache<CacheKey, ClassLoader> classLoaders;
  private final LocationFactory locationFactory;
  private final File tmpDir;

  public DirectoryClassLoaderProvider(CConfiguration cConf,
                                      LocationFactory locationFactory) {
    this.locationFactory = locationFactory;
    this.classLoaders = CacheBuilder.newBuilder()
      .removalListener(new ClassLoaderRemovalListener())
      .build(new ClassLoaderCacheLoader());
    File baseDir =
      new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR), cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    this.tmpDir = DirUtils.createTempDir(baseDir);
  }

  @Override
  public ClassLoader get(DatasetModuleMeta moduleMeta, ClassLoader parentClassLoader) throws IOException {
    URI jarLocation = moduleMeta.getJarLocation();
    return jarLocation == null ?
      parentClassLoader : classLoaders.getUnchecked(new CacheKey(jarLocation, parentClassLoader));
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
  
  private static final class ClassLoaderRemovalListener implements RemovalListener<CacheKey, ClassLoader> {
    @Override
    public void onRemoval(RemovalNotification<CacheKey, ClassLoader> notification) {
      ClassLoader cl = notification.getValue();
      if (cl instanceof Closeable) {
        Closeables.closeQuietly((Closeable) cl);
      }
    }
  }

  private final class CacheKey {
    private final URI uri;
    private final ClassLoader parentClassLoader;

    public CacheKey(URI uri, ClassLoader parentClassLoader) {
      this.uri = uri;
      this.parentClassLoader = parentClassLoader;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      CacheKey that = (CacheKey) o;

      return Objects.equal(this.uri, that.uri);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(uri);
    }
  }

  /**
   * A CacheLoader that will copy the jar to a temporary location, unpack it, and create a DirectoryClassLoader.
   */
  private final class ClassLoaderCacheLoader extends CacheLoader<CacheKey, ClassLoader> {

    @Override
    public ClassLoader load(CacheKey key) throws Exception {
      if (key.uri == null) {
        return key.parentClassLoader;
      }
      Location jarLocation = locationFactory.create(key.uri);
      File unpackedDir = DirUtils.createTempDir(tmpDir);
      BundleJarUtil.unJar(jarLocation, unpackedDir);
      LOG.trace("unpacking dataset jar from {} to {}.", key.uri.toString(), unpackedDir.getAbsolutePath());

      return new DirectoryClassLoader(unpackedDir, key.parentClassLoader, "lib");
    }
  }
}
