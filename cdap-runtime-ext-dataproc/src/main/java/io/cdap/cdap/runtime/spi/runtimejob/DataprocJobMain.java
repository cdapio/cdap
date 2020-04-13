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

import org.apache.twill.internal.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
    if (args.length < 2) {
      throw new RuntimeException("An implementation of RuntimeJob classname and spark compat should be provided as " +
                                   "job arguments.");
    }

    Thread.setDefaultUncaughtExceptionHandler((t, e) -> LOG.error("Uncaught exception from thread {}", t, e));

    // expand archive jars. This is needed because of CDAP-16456
    expandArchives(Arrays.asList(args).subList(2, args.length));

    String runtimeJobClassName = args[0];
    String sparkCompat = args[1];

    ClassLoader cl = DataprocJobMain.class.getClassLoader();
    if (!(cl instanceof URLClassLoader)) {
      throw new RuntimeException("Classloader is expected to be an instance of URLClassLoader");
    }

    // create classpath from resources, application and twill jars
    URL[] urls = getClasspath((URLClassLoader) cl, Arrays.asList(Constants.Files.RESOURCES_JAR,
                                                                 Constants.Files.APPLICATION_JAR,
                                                                 Constants.Files.TWILL_JAR));
    Arrays.stream(urls).forEach(url -> LOG.debug("Classpath URL: {}", url));

    // create new URL classloader with provided classpath
    try (URLClassLoader newCL = new URLClassLoader(urls, cl.getParent())) {
      Thread.currentThread().setContextClassLoader(newCL);

      // load environment class and create instance of it
      String dataprocEnvClassName = DataprocRuntimeEnvironment.class.getName();
      Class<?> dataprocEnvClass = newCL.loadClass(dataprocEnvClassName);
      Object newDataprocEnvInstance = dataprocEnvClass.newInstance();

      try {
        // call initialize() method on dataprocEnvClass
        Method initializeMethod = dataprocEnvClass.getMethod("initialize", String.class);
        LOG.info("Invoking initialize() on {} with {}", dataprocEnvClassName, sparkCompat);
        initializeMethod.invoke(newDataprocEnvInstance, sparkCompat);

        // call run() method on runtimeJobClass
        Class<?> runEnvCls = newCL.loadClass(RuntimeJobEnvironment.class.getName());
        Class<?> runnerCls = newCL.loadClass(runtimeJobClassName);
        Method runMethod = runnerCls.getMethod("run", runEnvCls);
        Method stopMethod = runnerCls.getMethod("requestStop");

        Object runner = runnerCls.newInstance();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
          try {
            stopMethod.invoke(runner);
          } catch (Exception e) {
            LOG.error("Exception raised when calling {}.stop()", runtimeJobClassName, e);
          }
        }));

        LOG.info("Invoking run() on {}", runtimeJobClassName);
        runMethod.invoke(runner, newDataprocEnvInstance);
      } finally {
        // call destroy() method on envProviderClass
        Method closeMethod = dataprocEnvClass.getMethod("destroy");
        LOG.info("Invoking destroy() on {}", runtimeJobClassName);
        closeMethod.invoke(newDataprocEnvInstance);
      }

      LOG.info("Runtime job completed.");
    } catch (Throwable t) {
      // We log here and rethrow to make sure the exception log is captured in the job output
      LOG.error("Runtime job failed", t);
      throw t;
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
  private static URL[] getClasspath(URLClassLoader cl, List<String> jarFiles) throws IOException {
    URL[] urls = cl.getURLs();
    List<URL> urlList = new ArrayList<>();
    for (String file : jarFiles) {
      File jarDir = new File(file);
      // add url for dir
      urlList.add(jarDir.toURI().toURL());
      if (file.equals(Constants.Files.RESOURCES_JAR)) {
        continue;
      }
      urlList.addAll(createClassPathURLs(jarDir));
    }

    urlList.addAll(Arrays.asList(urls));
    return urlList.toArray(new URL[0]);
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
    File[] files = dir.listFiles(f -> f.getName().endsWith(".jar"));
    if (files == null) {
      return;
    }
    for (File file : files) {
      result.add(file.toURI().toURL());
    }
  }


  private static void expandArchives(Collection<String> archiveNames) throws IOException {
    for (String archive : archiveNames) {
      unpack(Paths.get(archive));
    }
  }

  private static void unpack(Path archiveFile) throws IOException {
    if (!Files.isRegularFile(archiveFile)) {
      LOG.warn("Skip archive expansion due to {} is not a file", archiveFile);
      return;
    }
    unJar(archiveFile);
  }

  private static void unJar(Path archiveFile) throws IOException {
    Path targetDir = archiveFile.resolveSibling(archiveFile.getFileName() + ".tmp");
    LOG.debug("Expanding archive {} to {}", archiveFile, targetDir);

    try (ZipInputStream zipIn = new ZipInputStream(Files.newInputStream(archiveFile))) {
      Files.createDirectories(targetDir);

      ZipEntry entry;
      while ((entry = zipIn.getNextEntry()) != null) {
        Path output = targetDir.resolve(entry.getName());

        if (entry.isDirectory()) {
          Files.createDirectories(output);
        } else {
          Files.createDirectories(output.getParent());
          Files.copy(zipIn, output);
        }
      }
    }

    Files.deleteIfExists(archiveFile);
    Files.move(targetDir, archiveFile);
    LOG.debug("Archive expanded to {}", targetDir);
  }
}
