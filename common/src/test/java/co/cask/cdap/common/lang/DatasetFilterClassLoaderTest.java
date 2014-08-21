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
import co.cask.cdap.common.lang.jar.DatasetFilterClassLoader;
import co.cask.cdap.common.lang.jar.JarFinder;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Test;

import java.util.List;

/**
 *
 */
public class DatasetFilterClassLoaderTest {

  @Test
  public void testExposedDataset() throws ClassNotFoundException {
    String jarPath = JarFinder.getJar(ExposedDataset.class);
    ClassLoader filterParent = Objects.firstNonNull(Thread.currentThread().getContextClassLoader(),
                                                    ClassLoaders.class.getClassLoader());
    List<Location> datasetJars = Lists.newArrayList();
    LocationFactory lf = new LocalLocationFactory();
    datasetJars.add(lf.create(jarPath));
    ClassLoader dsClassLoader = new DatasetFilterClassLoader(datasetJars, filterParent);
    dsClassLoader.loadClass(ExposedDataset.class.getName());
  }

  @Test(expected = ClassNotFoundException.class)
  public void testUnExposedDataset() throws ClassNotFoundException {
    String jarPath = JarFinder.getJar(UnExposedDataset.class);
    List<Location> datasetJars = Lists.newArrayList();
    LocationFactory lf = new LocalLocationFactory();
    datasetJars.add(lf.create(jarPath));
    ClassLoader dsClassLoader = new DatasetFilterClassLoader(datasetJars, null);
    dsClassLoader.loadClass(UnExposedDataset.class.getName());
  }

  @ExposeDataset
  class ExposedDataset {

  }

  class UnExposedDataset {

  }
}
