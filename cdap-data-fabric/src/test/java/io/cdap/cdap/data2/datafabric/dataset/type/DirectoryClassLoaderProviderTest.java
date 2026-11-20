/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.data2.datafabric.dataset.type;

import com.google.common.cache.LoadingCache;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.DirectoryClassLoader;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.data2.datafabric.dataset.service.TestModule1;
import io.cdap.cdap.proto.DatasetModuleMeta;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link DirectoryClassLoaderProvider} caching and quiet close on cache removal.
 */
public class DirectoryClassLoaderProviderTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testGetCachesDirectoryClassLoaderAndCloseClosesQuietlyEvenOnException()
      throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());

    Location jar = AppJarHelper.createDeploymentJar(locationFactory, TestModule1.class);
    DatasetModuleMeta withJarMeta = new DatasetModuleMeta(
        "m1", TestModule1.class.getName(), jar.toURI(),
        Collections.singletonList("datasetType1"), Collections.emptyList());
    DatasetModuleMeta nullJarMeta = new DatasetModuleMeta(
        "mSys", TestModule1.class.getName(), null,
        Collections.singletonList("datasetType1"), Collections.emptyList());

    DirectoryClassLoaderProvider provider =
        new DirectoryClassLoaderProvider(cConf, locationFactory);

    ClassLoader parentCl = getClass().getClassLoader();
    Assert.assertSame(parentCl, provider.get(nullJarMeta, parentCl));

    ClassLoader loadedCl1 = provider.get(withJarMeta, parentCl);
    ClassLoader loadedCl2 = provider.get(withJarMeta, parentCl);
    Assert.assertTrue(loadedCl1 instanceof DirectoryClassLoader);
    Assert.assertSame(loadedCl1, loadedCl2);

    // Also insert a Closeable ClassLoader that throws IOException on close() into classLoaders cache
    // so ClassLoaderRemovalListener.onRemoval exercises Closeables.closeQuietly error swallowing.
    final AtomicBoolean throwingClClosed = new AtomicBoolean(false);
    ClassLoader throwingCloseCl = new ThrowingCloseableClassLoader(parentCl, throwingClClosed);

    Field cacheField = DirectoryClassLoaderProvider.class.getDeclaredField("classLoaders");
    cacheField.setAccessible(true);
    @SuppressWarnings("unchecked")
    LoadingCache<Object, ClassLoader> cache =
        (LoadingCache<Object, ClassLoader>) cacheField.get(provider);

    Class<?> cacheKeyClass = null;
    for (Class<?> declared : DirectoryClassLoaderProvider.class.getDeclaredClasses()) {
      if ("CacheKey".equals(declared.getSimpleName())) {
        cacheKeyClass = declared;
        break;
      }
    }
    Assert.assertNotNull(cacheKeyClass);
    Constructor<?> keyCtor = cacheKeyClass.getDeclaredConstructor(
        DirectoryClassLoaderProvider.class, URI.class, ClassLoader.class);
    keyCtor.setAccessible(true);
    Object syntheticKey = keyCtor.newInstance(provider, URI.create("file:///synthetic.jar"), parentCl);
    cache.put(syntheticKey, throwingCloseCl);

    // close() must swallow the IOException from throwingCloseCl.close() and clean up all entries
    provider.close();
    Assert.assertTrue(throwingClClosed.get());
    Assert.assertEquals(0, cache.size());
  }

  private static final class ThrowingCloseableClassLoader extends ClassLoader implements Closeable {
    private final AtomicBoolean closed;

    ThrowingCloseableClassLoader(ClassLoader parent, AtomicBoolean closed) {
      super(parent);
      this.closed = closed;
    }

    @Override
    public void close() throws IOException {
      closed.set(true);
      throw new IOException("simulated classloader close failure");
    }
  }
}
