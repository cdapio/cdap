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

import co.cask.cdap.common.utils.DirUtils;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

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

  private final Manifest manifest;

  public DirectoryClassLoader(File dir, ClassLoader parent, String...libDirs) {
    this(dir, parent, ImmutableSet.copyOf(libDirs));
  }

  public DirectoryClassLoader(File dir, ClassLoader parent, Iterable<String> libDirs) {
    super(getClassPathURLs(dir, ImmutableSet.copyOf(libDirs)), parent);

    // Try to load the Manifest from the unpacked directory
    Manifest manifest = null;
    try {
      InputStream input = new FileInputStream(new File(dir, JarFile.MANIFEST_NAME.replace('/', File.separatorChar)));
      try {
        manifest = new Manifest(input);
      } finally {
        Closeables.closeQuietly(input);
      }
    } catch (IOException e) {
      // Ignore, since it's possible that there is no MANIFEST
      LOG.trace("No Manifest file under {}", dir, e);
    }
    this.manifest = manifest;
  }

  /**
   * Returns the {@link Manifest} in the program jar or {@code null} if there is no manifest available.
   */
  @Nullable
  public Manifest getManifest() {
    return manifest;
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
    for (File file : DirUtils.listFiles(dir, "jar")) {
      result.add(file.toURI().toURL());
    }
  }
}
