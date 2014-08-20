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
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedList;
import java.util.List;

/**
 * Classloader that loads the given class, checks if it has {@link co.cask.cdap.api.annotation.ExposeDataset} annotation
 * if it has, it loads the class otherwise delegates to the parent classloader
 */
public class DatasetFilterClassLoader extends URLClassLoader {
  private final ClassLoader parentClassLoader;
  private static final Logger LOG = LoggerFactory.getLogger(DatasetFilterClassLoader.class);


  public DatasetFilterClassLoader(List<Location> datasetTypeJars, ClassLoader parentClassLoader) {
    super(getDatasetTypeUrls(datasetTypeJars), parentClassLoader);
    this.parentClassLoader = parentClassLoader;
  }

  private static URL[] getDatasetTypeUrls(List<Location> datasetTypeJars) {
    List<URL> datasetUrls = Lists.newLinkedList();
    File unpackedLocation = Files.createTempDir();
    int index = 0;
    try {
      for (Location datasetType : datasetTypeJars) {
        File temp = new File(unpackedLocation, String.valueOf(index++));
        temp.mkdir();
        if (datasetType != null) {
          BundleJarUtil.unpackProgramJar(datasetType, temp);
          datasetUrls.addAll(getClassPathUrls(temp));
        }
      }
    } catch (Exception e) {
      LOG.warn("Exception while unpacking dataset jar");
    }
    return datasetUrls.toArray(new URL[datasetUrls.size()]);
  }

  @Override
  public Class<?> findClass(String name) throws ClassNotFoundException {
    Class<?> dataset = getClass().getClassLoader().loadClass(name);

    ExposeDataset dsExpose = dataset.getAnnotation(ExposeDataset.class);
    if (dsExpose != null) {
      return dataset;
    } else {
      return parentClassLoader.loadClass(name);
    }
  }

  private static List<URL> getClassPathUrls(File unpackedJarDir) {
    List<URL> classPathUrls = new LinkedList<URL>();

    try {
      classPathUrls.add(unpackedJarDir.toURI().toURL());
    } catch (MalformedURLException e) {
      LOG.error("Error in adding unpackedJarDir to classPathUrls", e);
    }

    try {
      classPathUrls.addAll(getJarURLs(unpackedJarDir));
    } catch (MalformedURLException e) {
      LOG.error("Error in adding jar URLs to classPathUrls", e);
    }

    try {
      classPathUrls.addAll(getJarURLs(new File(unpackedJarDir, "lib")));
    } catch (MalformedURLException e) {
      LOG.error("Error in adding jar URLs to classPathUrls", e);
    }

    return classPathUrls;
  }

  private static List<URL> getJarURLs(File dir) throws MalformedURLException {
    File[] files = dir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".jar");
      }
    });
    List<URL> urls = new LinkedList<URL>();

    if (files != null) {
      for (File file : files) {
        urls.add(file.toURI().toURL());
      }
    }
    return urls;
  }

}
