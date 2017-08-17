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
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
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
public class DirectoryClassLoader extends InterceptableClassLoader {

  private static final Logger LOG = LoggerFactory.getLogger(DirectoryClassLoader.class);

  private final Manifest manifest;

  public DirectoryClassLoader(File dir, ClassLoader parent, String...libDirs) {
    this(dir, "", parent, ImmutableSet.copyOf(libDirs));
  }

  public DirectoryClassLoader(File dir, @Nullable String extraClassPath, ClassLoader parent, String...libDirs) {
    this(dir, extraClassPath, parent, Arrays.asList(libDirs));
  }

  public DirectoryClassLoader(File dir, @Nullable String extraClassPath, ClassLoader parent, Iterable<String> libDirs) {
    super(getClassPathURLs(dir, extraClassPath, ImmutableSet.copyOf(libDirs)), parent);

    // Try to load the Manifest from the unpacked directory
    Manifest manifest = null;
    try {
      try (
        InputStream input = new FileInputStream(new File(dir, JarFile.MANIFEST_NAME.replace('/', File.separatorChar)))
      ) {
        manifest = new Manifest(input);
      }
    } catch (IOException e) {
      // Ignore, since it's possible that there is no MANIFEST
      LOG.trace("No Manifest file under {}", dir, e);
    }
    this.manifest = manifest;
  }

  /**
   * Returns the {@link Manifest} in the directory if it contains the file
   * {@link JarFile#MANIFEST_NAME} or {@code null} if there is no manifest available.
   */
  @Nullable
  public Manifest getManifest() {
    return manifest;
  }

  /**
   * Always return {@code false} as this class won't do any class rewriting. Subclasses overriding this method
   * should also override {@link #rewriteClass(String, InputStream)}.
   */
  @Override
  protected boolean needIntercept(String className) {
    return false;
  }

  @Override
  public byte[] rewriteClass(String className, InputStream input) throws IOException {
    throw new UnsupportedOperationException("Class rewriting of class '" + className + "' is not supported");
  }

  private static URL[] getClassPathURLs(File dir, @Nullable String extraClassPath, Set<String> libDirs) {
    try {
      List<URL> urls = Lists.newArrayList(dir.toURI().toURL());
      addJarURLs(dir, urls);

      for (String libDir : libDirs) {
        addJarURLs(new File(dir, libDir), urls);
      }

      if (extraClassPath != null) {
        for (String path : Splitter.on(File.pathSeparatorChar).omitEmptyStrings().split(extraClassPath)) {
          String wildcardSuffix = File.separator + "*";

          if (path.endsWith(wildcardSuffix)) {
            addJarURLs(new File(path.substring(0, path.length() - wildcardSuffix.length())), urls);
          } else {
            urls.add(new File(path).toURI().toURL());
          }
        }
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
