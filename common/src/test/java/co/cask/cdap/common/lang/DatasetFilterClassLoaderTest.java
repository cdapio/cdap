/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.common.lang;

import co.cask.cdap.api.annotation.ExposeDataset;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.lang.jar.DatasetFilterClassLoader;
import co.cask.cdap.common.lang.jar.JarFinder;
import co.cask.cdap.common.utils.DirUtils;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Testing the exposed and unexposed annotations for Dataset Filtered Class loading
 */
public class DatasetFilterClassLoaderTest {

  @Test
  public void testExposedDataset() throws ClassNotFoundException, IOException {
    String jarPath = JarFinder.getJar(ExposedDataset.class);
    ClassLoader filterParent = Objects.firstNonNull(Thread.currentThread().getContextClassLoader(),
                                                    ClassLoaders.class.getClassLoader());
    File dsFile = Files.createTempDir();
    try {
      LocationFactory lf = new LocalLocationFactory();
      BundleJarUtil.unpackProgramJar(lf.create(jarPath), dsFile);
      ClassLoader dsClassLoader = new DatasetFilterClassLoader(dsFile, filterParent);
      dsClassLoader.loadClass(ExposedDataset.class.getName());
    } catch (IOException e) {
      throw Throwables.propagate(e);
    } finally {
      DirUtils.deleteDirectoryContents(dsFile);
    }
  }

  @Test(expected = ClassNotFoundException.class)
  public void testUnExposedDataset() throws ClassNotFoundException, IOException {
    String jarPath = JarFinder.getJar(UnExposedDataset.class);
    LocationFactory lf = new LocalLocationFactory();
    File dsFile = Files.createTempDir();
    try {
      BundleJarUtil.unpackProgramJar(lf.create(jarPath), dsFile);
      ClassLoader dsClassLoader = new DatasetFilterClassLoader(dsFile, null);
      dsClassLoader.loadClass(UnExposedDataset.class.getName());
    } catch (IOException e) {
      throw Throwables.propagate(e);
    } finally {
      DirUtils.deleteDirectoryContents(dsFile);
    }

  }

  @ExposeDataset
  class ExposedDataset {

  }

  class UnExposedDataset {

  }
}
