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

import co.cask.cdap.proto.DatasetModuleMeta;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * Unit test for testing {@link DatasetTypeClassLoaderFactory}.
 */
public class DatasetTypeClassLoaderTest {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetTypeClassLoaderTest.class);

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testJarReuse() throws Exception {
    LocationFactory locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    DatasetTypeClassLoaderFactory factory = new DistributedDatasetTypeClassLoaderFactory(locationFactory);

    File jarFile = createDummyJar(TMP_FOLDER.newFile());
    String name = DummyClass.class.getName();
    DatasetModuleMeta moduleMeta = new DatasetModuleMeta(name, name, jarFile.toURI(),
                                                         ImmutableList.<String>of(), ImmutableList.<String>of());

    // Create a ClassLoader, the jar should get expanded into a tmp directory
    URLClassLoader classLoader = (URLClassLoader) factory.create(moduleMeta, null);
    Set<URL> urls = ImmutableSet.copyOf(classLoader.getURLs());

    // Create another ClassLoader from the same jar. It must be reusing the same tmp directory as the previously created
    classLoader = (URLClassLoader) factory.create(moduleMeta, null);
    Assert.assertEquals(urls, ImmutableSet.copyOf(classLoader.getURLs()));

    // Now create another jar (with the same jar file name), the checksum should changed
    jarFile.delete();
    createDummyJar(jarFile);

    // Create a ClassLoader from the updated jar. It must use a new tmp directory.
    classLoader = (URLClassLoader) factory.create(moduleMeta, null);
    Assert.assertNotEquals(urls, ImmutableSet.copyOf(classLoader.getURLs()));
  }

  @Test
  public void testConcurrent() throws Exception {
    LocationFactory locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    final DatasetTypeClassLoaderFactory factory = new DistributedDatasetTypeClassLoaderFactory(locationFactory);

    File jarFile = createDummyJar(TMP_FOLDER.newFile());
    String name = DummyClass.class.getName();
    final DatasetModuleMeta moduleMeta = new DatasetModuleMeta(name, name, jarFile.toURI(),
                                                               ImmutableList.<String>of(), ImmutableList.<String>of());

    // Starts n-threads to create the dataset ClassLoader concurrently
    int threads = 5;
    final CyclicBarrier cyclicBarrier = new CyclicBarrier(threads);
    final Set<Class<?>> classesLoaded = Sets.newCopyOnWriteArraySet();
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    for (int i = 0; i < threads; i++) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            cyclicBarrier.await();
            URLClassLoader classLoader = (URLClassLoader) factory.create(moduleMeta, null);
            classesLoaded.add(classLoader.loadClass(DummyClass.class.getName()));
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      });
    }

    executor.shutdown();
    Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

    // There should different classes loaded, since they loaded from different ClassLoader
    // However, the file paths for the underlying URLClassLoader should be the same
    Assert.assertEquals(5, classesLoaded.size());

    Set<URL> urls = Sets.newHashSet();
    for (Class<?> cls: classesLoaded) {
      urls.addAll(Arrays.asList(((URLClassLoader) cls.getClassLoader()).getURLs()));
    }

    Assert.assertEquals(1, urls.size());
  }


  private File createDummyJar(File file) throws Exception {
    // Just create an jar containing this test class and a timestamp in the manifest
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, Long.toString(System.nanoTime()));

    JarOutputStream  output = new JarOutputStream(new FileOutputStream(file), manifest);
    try {
      String resourceName = DummyClass.class.getName().replace('.', '/') + ".class";
      URL resource = Resources.getResource(resourceName);
      output.putNextEntry(new JarEntry(resourceName));
      ByteStreams.copy(Resources.newInputStreamSupplier(resource), output);
      return file;
    } finally {
      output.close();
    }
  }
}
