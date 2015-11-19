/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.launcher;

import org.apache.twill.internal.Constants;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

/**
 * A launcher for application from a archive jar.
 * This class should have no dependencies on any library except the J2SE one.
 * This class should not import any thing except java.*
 */
public final class TwillLauncher {

  private static final int TEMP_DIR_ATTEMPTS = 20;

  /**
   * Main method to unpackage a jar and run the mainClass.main() method.
   * @param args args[0] is the path to jar file, args[1] is the class name of the mainClass.
   *             The rest of args will be passed the mainClass unmodified.
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.out.println("Usage: java " + TwillLauncher.class.getName() + " [jarFile] [mainClass] [use_classpath]");
      return;
    }

    File file = new File(args[0]);
    final File targetDir = createTempDir("twill.launcher");

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        System.out.println("Cleanup directory " + targetDir);
        deleteDir(targetDir);
      }
    });

    System.out.println("UnJar " + file + " to " + targetDir);
    unJar(file, targetDir);

    // Create ClassLoader
    URLClassLoader classLoader = createClassLoader(targetDir, Boolean.parseBoolean(args[2]));
    Thread.currentThread().setContextClassLoader(classLoader);

    System.out.println("Launch class (" + args[1] + ") with classpath: " + Arrays.toString(classLoader.getURLs()));

    Class<?> mainClass = classLoader.loadClass(args[1]);
    Method mainMethod = mainClass.getMethod("main", String[].class);
    String[] arguments = Arrays.copyOfRange(args, 3, args.length);
    System.out.println("Launching main: " + mainMethod + " " + Arrays.toString(arguments));
    mainMethod.invoke(mainClass, new Object[]{arguments});
    System.out.println("Main class completed.");

    System.out.println("Launcher completed");
  }

  /**
   * This method is copied from Guava Files.createTempDir().
   */
  private static File createTempDir(String prefix) throws IOException {
    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    if (!baseDir.isDirectory() && !baseDir.mkdirs()) {
      throw new IOException("Tmp directory not exists: " + baseDir.getAbsolutePath());
    }

    String baseName = prefix + "-" + System.currentTimeMillis() + "-";

    for (int counter = 0; counter < TEMP_DIR_ATTEMPTS; counter++) {
      File tempDir = new File(baseDir, baseName + counter);
      if (tempDir.mkdir()) {
        return tempDir;
      }
    }
    throw new IOException("Failed to create directory within "
                            + TEMP_DIR_ATTEMPTS + " attempts (tried "
                            + baseName + "0 to " + baseName + (TEMP_DIR_ATTEMPTS - 1) + ')');
  }

  private static void unJar(File jarFile, File targetDir) throws IOException {
    try (JarInputStream jarInput = new JarInputStream(new FileInputStream(jarFile))) {
      JarEntry jarEntry = jarInput.getNextJarEntry();
      while (jarEntry != null) {
        File target = new File(targetDir, jarEntry.getName());
        if (jarEntry.isDirectory()) {
          target.mkdirs();
        } else {
          target.getParentFile().mkdirs();
          copy(jarInput, target);
        }
        jarEntry = jarInput.getNextJarEntry();
      }
    }
  }

  private static void copy(InputStream is, File file) throws IOException {
    byte[] buf = new byte[8192];
    try (OutputStream os = new BufferedOutputStream(new FileOutputStream(file))) {
      int len = is.read(buf);
      while (len != -1) {
        os.write(buf, 0, len);
        len = is.read(buf);
      }
    }
  }

  private static URLClassLoader createClassLoader(File dir, boolean useClassPath) {
    try {
      List<URL> urls = new ArrayList<>();
      urls.add(dir.toURI().toURL());
      urls.add(new File(dir, "classes").toURI().toURL());
      urls.add(new File(dir, "resources").toURI().toURL());

      File libDir = new File(dir, "lib");
      File[] files = libDir.listFiles();
      if (files != null) {
        List<File> fileList = Arrays.asList(files);
        Collections.sort(fileList);
        for (File file : fileList) {
          if (file.getName().endsWith(".jar")) {
            urls.add(file.toURI().toURL());
          }
        }
      }

      if (useClassPath) {
        addClassPathsToList(urls, Constants.CLASSPATH);
      }

      addClassPathsToList(urls, Constants.APPLICATION_CLASSPATH);

      return new URLClassLoader(urls.toArray(new URL[0]));

    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static void addClassPathsToList(List<URL> urls, String resource) throws IOException {
    try (InputStream is = ClassLoader.getSystemResourceAsStream(resource)) {
      if (is != null) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")))) {
          String line = reader.readLine();
          if (line != null) {
            for (String path : line.split(":")) {
              urls.addAll(getClassPaths(path.trim()));
            }
          }
        }
      }
    }
  }

  private static Collection<URL> getClassPaths(String path) throws MalformedURLException {
    String classpath = expand(path);
    if (classpath.endsWith("/*")) {
      // Grab all .jar files
      File dir = new File(classpath.substring(0, classpath.length() - 2));
      File[] files = dir.listFiles();
      if (files == null || files.length == 0) {
        return singleItem(dir.toURI().toURL());
      }

      List<File> fileList = Arrays.asList(files);
      Collections.sort(fileList);
      List<URL> result = new ArrayList<>(files.length);
      for (File file : fileList) {
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
    List<URL> result = new ArrayList<URL>(1);
    result.add(url);
    return result;
  }

  private static String expand(String value) {
    String result = value;
    for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
      result = result.replace("$" + entry.getKey(), entry.getValue());
      result = result.replace("${" + entry.getKey() + "}", entry.getValue());
    }
    return result;
  }

  private static void deleteDir(File dir) {
    File[] files = dir.listFiles();
    if (files == null || files.length == 0) {
      dir.delete();
      return;
    }
    for (File file : files) {
      deleteDir(file);
    }
    dir.delete();
  }
}
