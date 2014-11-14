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

package co.cask.cdap.common.lang;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Set;

/**
 * A {@link ClassLoader} that takes a directory of classes and jars as its class path.
 * <p/>
 * The URLs for class loading are:
 * <p/>
 * <pre>
 * [dir]
 * [dir]/*.jar
 * [dir]/[custom1]/*.jar
 * ...
 * </pre>
 */
public class DirectoryClassLoader extends URLClassLoader {

  private static final Logger LOG = LoggerFactory.getLogger(DirectoryClassLoader.class);
  private static final FilenameFilter JAR_FILE_FILTER = new FilenameFilter() {
    @Override
    public boolean accept(File dir, String name) {
      return name.endsWith(".jar");
    }
  };

  public DirectoryClassLoader(File dir, ClassLoader parent, String...libDirs) {
    this(dir, parent, ImmutableSet.copyOf(libDirs));
  }

  public DirectoryClassLoader(File dir, ClassLoader parent, Iterable<String> libDirs) {
    super(getClassPathURLs(dir, ImmutableSet.copyOf(libDirs)), parent);
  }

  private static URL[] getClassPathURLs(File dir, Set<String> libDirs) {
    try {
      List<URL> urls = Lists.newArrayList(dir.toURI().toURL());
      addJarURLs(dir, urls);

      for (String libDir : libDirs) {
        addJarURLs(new File(dir, libDir), urls);
      }
      return urls.toArray(new URL[urls.size()]);
    } catch (MalformedURLException e) {
      // Should never happen
      LOG.error("Error in adding jar URLs to classPathUrls", e);
      throw Throwables.propagate(e);
    }
  }

  private static void addJarURLs(File dir, List<URL> result) throws MalformedURLException {
    File[] files = dir.listFiles(JAR_FILE_FILTER);
    if (files == null) {
      return;
    }

    for (File file : files) {
      result.add(file.toURI().toURL());
    }
  }
}
