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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

/**
 *
 */
public class WrappedLauncher {

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

    //System.out.println("This URL: " + thisURL);

    File appJarDir = new File(Constants.Files.APPLICATION_JAR);
    File twillJarDir = new File(Constants.Files.TWILL_JAR);
    File resourceJarDir = new File(Constants.Files.RESOURCES_JAR);
    File runtimeConfigDir = new File(Constants.Files.RUNTIME_CONFIG_JAR);
    File logback = new File("logback.xml");

    // add app jar, twill jar, resource jar
    URL[] classpath = createClasspath(appJarDir, twillJarDir, resourceJarDir, runtimeConfigDir, logback);
    List<URL> urlList = new ArrayList<>(Arrays.asList(urls));

    // add this url
    Deque<URL> queue = new LinkedList<>(urlList);

    for (URL url : classpath) {
      System.out.println("URL: " + url);
        if (url.toString().endsWith(".jar")) {
          addAll(url, queue);
          queue.addFirst(url);
      } else {
        queue.addLast(url);
      }
    }

   // System.out.println("Classpath URLs: " + queue);

    URLClassLoader newCL = new URLClassLoader(queue.toArray(new URL[0]), cl.getParent());
    Thread.currentThread().setContextClassLoader(newCL);
    Class<?> cls = newCL.loadClass(LauncherRunner.class.getName());
    Method method = cls.getMethod("runnerMethod", String[].class);

    System.out.println("Invoking runnerMethod.");

    System.out.println("Launching main: doMain : " + Arrays.toString(args));
    method.invoke(cls.newInstance(), new Object[]{args});

    System.out.println("Main class completed.");
    System.out.println("Launcher completed");
  }

  private static URL[] createClasspath(File appJarDir, File twillJarDir,
                                       File resourceJarDir, File runtimeConfigDir, File logback) throws IOException {
    List<URL> urls = new ArrayList<>();

    // For backward compatibility, sort jars from twill and jars from application together
    // With TWILL-179, this will change as the user can have control on how it should be.
    List<File> libJarFiles = listJarFiles(new File(appJarDir, "lib"), new ArrayList<>());
    Collections.sort(listJarFiles(new File(twillJarDir, "lib"), libJarFiles), Comparator.comparing(File::getName));

    // Add the app jar, resources jar and twill jar directories to the classpath as well
    for (File dir : Arrays.asList(appJarDir, twillJarDir)) {
      urls.add(dir.toURI().toURL());
      urls.add(new File(dir, "classes").toURI().toURL());
    }

    // add resources and runtime args
    urls.add(new File(resourceJarDir, "resources").toURI().toURL());
    addRuntimeConfig(runtimeConfigDir.toURI().toURL(), urls);

    // Add all lib jars
    for (File jarFile : libJarFiles) {
      urls.add(jarFile.toURI().toURL());
    }

    //urls.add(logback.toURI().toURL());
    return urls.toArray(new URL[urls.size()]);
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

       //   System.out.println("Jar entry" + entry.getName() + "is expanded to " + jarPath);
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

  private static void addRuntimeConfig(URL jarURL, List<URL> urls) throws IOException {
    Path tempDir = Files.createTempDirectory("expanded.jar");
    List<URL> depJars = new ArrayList<>();
    try (JarInputStream jarInput = new JarInputStream(jarURL.openStream())) {
      JarEntry entry = jarInput.getNextJarEntry();
      while (entry != null) {
        String name = entry.getName();
        int idx = name.lastIndexOf("/");
        if (idx >= 0) {
          name = name.substring(idx + 1);
        }
        Path jarPath = tempDir.resolve(name);

       // System.out.println("Jar entry" + entry.getName() + "is expanded to " + jarPath);
        Files.copy(jarInput, jarPath);
        depJars.add(jarPath.toUri().toURL());
        entry = jarInput.getNextJarEntry();
      }
    }

    ListIterator<URL> itor = depJars.listIterator(depJars.size());
    while (itor.hasPrevious()) {
      urls.add(itor.previous());
    }
  }
}
