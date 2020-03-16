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

package io.cdap.cdap.runtime.spi.runtimejob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Main class that will be called from dataproc driver.
 */
public class DataprocJobMain {
  private static final Logger LOG = LoggerFactory.getLogger(DataprocJobMain.class);

  /**
   * Main method to setup classpath and call the RuntimeJob.run() method.
   *
   * @param args the name of implementation of RuntimeJob class
   * @throws Exception any exception while running the job
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      throw new RuntimeException("An implementation of RuntimeJob classname should be provided as an argument.");
    }

    String runtimeJobClassName = args[0];

    ClassLoader cl = DataprocJobMain.class.getClassLoader();
    if (!(cl instanceof URLClassLoader)) {
      throw new RuntimeException("Classloader is expected to be an instance of URLClassLoader");
    }

    // get classpath
    Path tempDir = Files.createTempDirectory("expanded.jars");
    URL[] urls = getClasspath((URLClassLoader) cl, tempDir.toFile());

    // create new URL classloader with provided classpath
    try (URLClassLoader newCL = new URLClassLoader(urls, cl.getParent())) {
      Thread.currentThread().setContextClassLoader(newCL);

      // load environment class and create instance of it
      String dataprocEnvClassName = DataprocRuntimeEnvironment.class.getName();
      Class<?> dataprocEnvClass = newCL.loadClass(dataprocEnvClassName);
      Object newDataprocEnvInstance = dataprocEnvClass.newInstance();

      try {
        // call initialize() method on dataprocEnvClass
        Method initializeMethod = dataprocEnvClass.getMethod("initialize");
        LOG.info("Invoking initialize() on {}", dataprocEnvClassName);
        initializeMethod.invoke(newDataprocEnvInstance);

        // call run() method on runtimeJobClass
        Class<?> runEnvCls = newCL.loadClass(RuntimeJobEnvironment.class.getName());
        Class<?> runner = newCL.loadClass(runtimeJobClassName);
        Method method = runner.getMethod("run", runEnvCls);
        LOG.info("Invoking run() on {}", runtimeJobClassName);
        method.invoke(runner.newInstance(), newDataprocEnvInstance);

      } finally {
        // call destroy() method on envProviderClass
        Method closeMethod = dataprocEnvClass.getMethod("destroy");
        LOG.info("Invoking destroy() on {}", runtimeJobClassName);
        closeMethod.invoke(newDataprocEnvInstance);
        // make sure directory is cleaned before exiting
        deleteDirectoryContents(tempDir.toFile());
      }

      LOG.info("Runtime job completed.");
    }
  }

  /**
   * This method will generate class path by adding following to urls to front of default classpath:
   *
   * expanded.resource.jar
   * expanded.application.jar
   * expanded.application.jar/lib/*.jar
   * expanded.application.jar/classes
   * expanded.twill.jar
   * expanded.twill.jar/lib/*.jar
   * expanded.twill.jar/classes
   *
   */
  private static URL[] getClasspath(URLClassLoader cl, File tempDir) throws IOException {
    URL[] urls = cl.getURLs();
    List<URL> urlList = new ArrayList<>();
    for (String file : Arrays.asList(Constants.RESOURCES_JAR, Constants.APPLICATION_JAR, Constants.TWILL_JAR)) {
      File jar = new File(file);
      File jarDir = new File(tempDir, "expanded." + file);
      expand(jar, jarDir);
      // add url for dir
      urlList.add(jarDir.toURI().toURL());
      if (file.equals(Constants.RESOURCES_JAR)) {
        continue;
      }
      urlList.addAll(createClassPathURLs(jarDir));
    }

    urlList.addAll(Arrays.asList(urls));

    LOG.info("Classpath URLs: {}", urlList);
    return urlList.toArray(new URL[0]);
  }

  /**
   * Expands jar into destination directory.
   */
  private static void expand(File jarFile, File destDir) throws IOException {
    try (ZipInputStream zipIn = new ZipInputStream(new BufferedInputStream(new FileInputStream(jarFile)))) {
      Path targetPath = destDir.toPath();
      Files.createDirectories(targetPath);

      ZipEntry entry;
      while ((entry = zipIn.getNextEntry()) != null) {
        Path output = targetPath.resolve(entry.getName());

        if (entry.isDirectory()) {
          Files.createDirectories(output);
        } else {
          Files.createDirectories(output.getParent());
          Files.copy(zipIn, output);
        }
      }
    }
  }

  private static List<URL> createClassPathURLs(File dir) throws MalformedURLException {
    List<URL> urls = new ArrayList<>();
    // add jar urls from lib under dir
    addJarURLs(new File(dir, "lib"), urls);
    // add classes under dir
    urls.add(new File(dir, "classes").toURI().toURL());
    return urls;
  }

  private static void addJarURLs(File dir, List<URL> result) throws MalformedURLException {
    if (dir.listFiles() == null) {
      return;
    }
    for (File file : dir.listFiles()) {
      if (file.getName().endsWith(".jar")) {
        result.add(file.toURI().toURL());
      }
    }
  }

  /**
   * Recursively deletes all the contents of the directory and the directory itself.
   */
  private static void deleteDirectoryContents(File file) {
    if (file.isDirectory()) {
      File[] entries = file.listFiles();
      if (entries != null) {
        for (File entry : entries) {
          deleteDirectoryContents(entry);
        }
      }
    }
    if (!file.delete()) {
      LOG.warn("Failed to delete temp file {}", file);
    }
  }
}
