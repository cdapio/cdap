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

package co.cask.cdap.common.lang.jar;

import co.cask.cdap.api.annotation.ExposeDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Classloader that loads the given class, checks if it has {@link co.cask.cdap.api.annotation.ExposeDataset} annotation
 * if it does not have the annotation , it throws {@link java.lang.ClassNotFoundException}
 */
public class DatasetFilterClassLoader extends ClassLoader {
  private final URL[] datasetUrls;
  private URLClassLoader datasetClassLoader;


  public DatasetFilterClassLoader(File datasetTypeJar, ClassLoader parentClassLoader) {
    super(parentClassLoader);
    this.datasetUrls = ClassPathUrlsUtil.getClassPathUrls(datasetTypeJar);
    this.datasetClassLoader = new URLClassLoader(datasetUrls, parentClassLoader);
  }

  @Override
  public Class<?> findClass(String name) throws ClassNotFoundException {
    Class<?> dataset = datasetClassLoader.loadClass(name);
    ExposeDataset dsExpose = dataset.getAnnotation(ExposeDataset.class);
    if (dsExpose != null) {
      return dataset;
    } else {
      throw new ClassNotFoundException("Trying to load an unExposed dataset class");
    }
  }
}
