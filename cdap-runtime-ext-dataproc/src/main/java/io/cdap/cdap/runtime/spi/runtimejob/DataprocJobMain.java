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
import java.lang.reflect.InvocationTargetException;
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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Main class that will be called from dataproc driver.
 */
public class DataprocJobMain {

  public static final String RUNTIME_JOB_CLASS = "runtimeJobClass";
  public static final String SPARK_COMPAT = "sparkCompat";
  public static final String ARCHIVE = "archive";
  public static final String PROPERTY_PREFIX = "prop";

  private static final Logger LOG = LoggerFactory.getLogger(DataprocJobMain.class);

  /**
   * Main method to setup classpath and call the RuntimeJob.run() method.
   *
   * @param args the name of implementation of RuntimeJob class
   * @throws Exception any exception while running the job
   */
  public static void main(String[] args) throws Exception {
    Map<String, Collection<String>> arguments = fromPosixArray(args);

    if (!arguments.containsKey(RUNTIME_JOB_CLASS)) {
      throw new RuntimeException("Missing --" + RUNTIME_JOB_CLASS + " argument for the RuntimeJob classname");
    }
    if (!arguments.containsKey(SPARK_COMPAT)) {
      throw new RuntimeException("Missing --" + SPARK_COMPAT + " argument for the spark compat version");
    }

    Thread.setDefaultUncaughtExceptionHandler((t, e) -> LOG.error("Uncaught exception from thread {}", t, e));

    // Get the Java properties
    for (Map.Entry<String, Collection<String>> entry : arguments.entrySet()) {
      if (entry.getKey().startsWith(PROPERTY_PREFIX)) {
        System.setProperty(entry.getKey().substring(PROPERTY_PREFIX.length()), entry.getValue().iterator().next());
      }
    }

    // expand archive jars. This is needed because of CDAP-16456
    expandArchives(arguments.getOrDefault(ARCHIVE, Collections.emptySet()));

    String runtimeJobClassName = arguments.get(RUNTIME_JOB_CLASS).iterator().next();
    String sparkCompat = arguments.get(SPARK_COMPAT).iterator().next();

    ClassLoader cl = DataprocJobMain.class.getClassLoader();
    if (!(cl instanceof URLClassLoader)) {
      throw new RuntimeException("Classloader is expected to be an instance of URLClassLoader");
    }

    // create classpath from resources, application and twill jars
    URL[] urls = getClasspath((URLClassLoader) cl, Arrays.asList(Constants.Files.RESOURCES_JAR,
                                                                 Constants.Files.APPLICATION_JAR,
                                                                 Constants.Files.TWILL_JAR));
    Arrays.stream(urls).forEach(url -> LOG.debug("Classpath URL: {}", url));

    // Create new URL classloader with provided classpath.
    // Don't close the classloader since this is the main classloader,
    // which can be used for shutdown hook execution.
    // Closing it too early can result in NoClassDefFoundError in shutdown hook execution.
    ClassLoader newCL = createContainerClassLoader(urls);
    try {
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

  /**
   * Converts a POSIX compliant program argument array to a String-to-String Map.
   * @param args Array of Strings where each element is a POSIX compliant program argument (Ex: "--os=Linux" )
   * @return Map of argument Keys and Values (Ex: Key = "os" and Value = "Linux").
   */
  private static Map<String, Collection<String>> fromPosixArray(String[] args) {
    Map<String, Collection<String>> result = new LinkedHashMap<>();
    for (String arg : args) {
      int idx = arg.indexOf('=');
      int keyOff = arg.startsWith("--") ? "--".length() : 0;
      String key = idx < 0 ? arg.substring(keyOff) : arg.substring(keyOff, idx);
      String value = idx < 0 ? "" : arg.substring(idx + 1);
      // Remote quote from the value if it is quoted
      if (value.length() >= 2 && value.charAt(0) == '"' && value.charAt(value.length() - 1) == '"') {
        value = value.substring(1, value.length() - 1);
      }

      result.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }
    return result;
  }

  /**
   * Creates a {@link ClassLoader} for the the job execution.
   */
  private static ClassLoader createContainerClassLoader(URL[] classpath) {
    String containerClassLoaderName = System.getProperty(Constants.TWILL_CONTAINER_CLASSLOADER);
    URLClassLoader classLoader = new URLClassLoader(classpath, DataprocJobMain.class.getClassLoader().getParent());
    if (containerClassLoaderName == null) {
      return classLoader;
    }

    try {
      @SuppressWarnings("unchecked")
      Class<? extends ClassLoader> cls = (Class<? extends ClassLoader>) classLoader.loadClass(containerClassLoaderName);

      // Instantiate with constructor (URL[] classpath, ClassLoader parentClassLoader)
      return cls.getConstructor(URL[].class, ClassLoader.class).newInstance(classpath, classLoader.getParent());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to load container class loader class " + containerClassLoaderName, e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Container class loader must have a public constructor with " +
                                   "parameters (URL[] classpath, ClassLoader parent)", e);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      throw new RuntimeException("Failed to create container class loader of class " + containerClassLoaderName, e);
    }
  }
}
