/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed.launcher;

import org.apache.twill.internal.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

/**
 *
 */
public class WrappedLauncher {
  private static final Logger LOG = LoggerFactory.getLogger(WrappedLauncher.class);

  public static void main(String[] args) throws Exception {
    ClassLoader cl = WrappedLauncher.class.getClassLoader();
    if (!(cl instanceof URLClassLoader)) {
      throw new RuntimeException("Expect it to be a URLClassLoader");
    }

    URL[] urls = ((URLClassLoader) cl).getURLs();
    URL thisURL = WrappedLauncher.class.getClassLoader().getResource(WrappedLauncher.class.getName()
                                                                    .replace('.', '/') + ".class");
    if (thisURL == null) {
      throw new RuntimeException("Failed to find the resource for main class");
    }
    if ("jar".equals(thisURL.getProtocol())) {
      String path = thisURL.getFile();
      thisURL = URI.create(path.substring(0, path.indexOf("!/"))).toURL();
    }

    LOG.info("This URL: {}", thisURL);

    File appJarDir = new File(Constants.Files.APPLICATION_JAR);
    File twillJarDir = new File(Constants.Files.TWILL_JAR);
    File resourceJarDir = new File(Constants.Files.RESOURCES_JAR);
    // TODO Expand and add runtime config jar to classpath

    // add app jar, twill jar, resource jar
    URL[] classpath = createClasspath(appJarDir, twillJarDir, resourceJarDir);
    List<URL> urlList = new ArrayList<>(Arrays.asList(urls));

    // add this url
    Deque<URL> queue = new LinkedList<>(urlList);

    for (URL url : classpath) {
      LOG.info("URL: {}", url);
        if (url.toString().endsWith(".jar")) {
          addAll(url, queue);
          queue.addFirst(url);
      } else {
        queue.addLast(url);
      }
    }

    LOG.info("Classpath URLs: {}", queue);

    URLClassLoader newCL = new URLClassLoader(queue.toArray(new URL[0]), cl.getParent());
    Thread.currentThread().setContextClassLoader(newCL);
    Class<?> cls = newCL.loadClass(LauncherRunner.class.getName());
    Method method = cls.getMethod("runnerMethod");

    LOG.info("Invoking runnerMethod");
    method.invoke(cls.newInstance());
    System.out.println("Main class completed.");
    System.out.println("Launcher completed");
  }

  private static URL[] createClasspath(File appJarDir, File twillJarDir, File resourceJarDir) throws IOException {
    List<URL> urls = new ArrayList<>();

    // For backward compatibility, sort jars from twill and jars from application together
    // With TWILL-179, this will change as the user can have control on how it should be.
    List<File> libJarFiles = listJarFiles(new File(appJarDir, "lib"), new ArrayList<File>());
    Collections.sort(listJarFiles(new File(twillJarDir, "lib"), libJarFiles), new Comparator<File>() {
      @Override
      public int compare(File file1, File file2) {
        // order by the file name only. If the name are the same, the one in application jar will prevail.
        return file1.getName().compareTo(file2.getName());
      }
    });

    // Add the app jar, resources jar and twill jar directories to the classpath as well
    for (File dir : Arrays.asList(appJarDir, twillJarDir)) {
      urls.add(dir.toURI().toURL());
      urls.add(new File(dir, "classes").toURI().toURL());
    }

    urls.add(new File(resourceJarDir, "resources").toURI().toURL());


    // Add all lib jars
    for (File jarFile : libJarFiles) {
      urls.add(jarFile.toURI().toURL());
    }

    return urls.toArray(new URL[urls.size()]);
  }

  private static List<URL> addClassPathsToList(List<URL> urls, File classpathFile) throws IOException {
    // TODO figure out a way to get classpath file and Set environment variable on dataproc job
    List<URL> testUrls = new ArrayList<>();
    String line = System.getenv("$HADOOP_CLASSPATH");
    if (line != null) {
      for (String path : expand(line).split(":")) {
        testUrls.addAll(getClassPaths(path.trim()));
      }
    }
    return testUrls;
  }

  private static Collection<URL> getClassPaths(String path) throws MalformedURLException {
    String classpath = expand(path);
    if (classpath.endsWith("/*")) {
      // Grab all .jar files
      File dir = new File(classpath.substring(0, classpath.length() - 2));
      List<File> files = listJarFiles(dir, new ArrayList<File>());
      Collections.sort(files);
      if (files.isEmpty()) {
        return singleItem(dir.toURI().toURL());
      }

      List<URL> result = new ArrayList<>(files.size());
      for (File file : files) {
        if (file.getName().endsWith(".jar")) {
          result.add(file.toURI().toURL());
        }
      }
      return result;
    } else {
      return singleItem(new File(classpath).toURI().toURL());
    }
  }

  private static Collection<URL> singleItem(URL url) {
    List<URL> result = new ArrayList<>(1);
    result.add(url);
    return result;
  }

  private static String expand(String value) {
    String result = value;
    for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
      result = result.replace("$" + entry.getKey(), entry.getValue());
      result = result.replace("${" + entry.getKey() + "}", entry.getValue());
    }
    LOG.info("Expand finished: {}", result);
    return result;
  }

  /**
   * Populates a list of {@link File} under the given directory that has ".jar" as extension.
   */
  private static List<File> listJarFiles(File dir, List<File> result) {
    File[] files = dir.listFiles();
    if (files == null || files.length == 0) {
      return result;
    }
    for (File file : files) {
      if (file.getName().endsWith(".jar")) {
        result.add(file);
      }
    }
    return result;
  }

  private static void addAll(URL jarURL, Deque<URL> urls) throws IOException {
    Path tempDir = Files.createTempDirectory("expanded.jar");
    List<URL> depJars = new ArrayList<>();
    try (JarInputStream jarInput = new JarInputStream(jarURL.openStream())) {
      JarEntry entry = jarInput.getNextJarEntry();
      while (entry != null) {
        if (entry.getName().endsWith(".jar")) {
          String name = entry.getName();
          int idx = name.lastIndexOf("/");
          if (idx >= 0) {
            name = name.substring(idx + 1);
          }
          Path jarPath = tempDir.resolve(name);

          LOG.info("Jar entry {} is expanded to {}", entry.getName(), jarPath);
          Files.copy(jarInput, jarPath);
          depJars.add(jarPath.toUri().toURL());
        }
        entry = jarInput.getNextJarEntry();
      }
    }

    ListIterator<URL> itor = depJars.listIterator(depJars.size());
    while (itor.hasPrevious()) {
      urls.addFirst(itor.previous());
    }
  }
}
