/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.distributed.remote;

import org.apache.twill.internal.Constants;
import org.apache.twill.launcher.TwillLauncher;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * This class is copied from {@link TwillLauncher} to workaround TWILL-259. The only differences is in the
 * {@link #addClassPathsToList(List, File)} method, that it expands a line from the classpath text file with the
 * environment variable before splitting it with ":".
 */
public class RemoteLauncher {

  /**
   * Main method to unpackage a jar and run the mainClass.main() method.
   * @param args args[0] is the class name of the mainClass, args[1] is a boolean, telling whether to append classpath
   *             from the "classpath.txt" runtime config jar or not. The rest of args are arguments to the mainClass.
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.out.println("Usage: java " + TwillLauncher.class.getName() + " [mainClass] [use_classpath] [args...]");
      return;
    }

    String mainClassName = args[0];
    boolean userClassPath = Boolean.parseBoolean(args[1]);

    // Create ClassLoader
    URL[] classpath = createClasspath(userClassPath);
    ClassLoader classLoader = createContainerClassLoader(classpath);
    System.out.println("Launch class (" + mainClassName + ") using classloader " + classLoader.getClass().getName()
                         + " with classpath: " + Arrays.toString(classpath));

    Thread.currentThread().setContextClassLoader(classLoader);

    Class<?> mainClass = classLoader.loadClass(mainClassName);
    Method mainMethod = mainClass.getMethod("main", String[].class);
    String[] arguments = Arrays.copyOfRange(args, 2, args.length);
    System.out.println("Launching main: " + mainMethod + " " + Arrays.toString(arguments));
    mainMethod.invoke(mainClass, new Object[]{arguments});
    System.out.println("Main class completed.");

    System.out.println("Launcher completed");
  }

  private static URL[] createClasspath(boolean useClassPath) throws IOException {
    List<URL> urls = new ArrayList<>();

    File appJarDir = new File(Constants.Files.APPLICATION_JAR);
    File resourceJarDir = new File(Constants.Files.RESOURCES_JAR);
    File twillJarDir = new File(Constants.Files.TWILL_JAR);

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

    // this indicates that we are not in the ApplicationMaster
    if (useClassPath) {
      urls.add(new File(resourceJarDir, "resources").toURI().toURL());
    }

    // Add all lib jars
    for (File jarFile : libJarFiles) {
      urls.add(jarFile.toURI().toURL());
    }

    if (useClassPath) {
      addClassPathsToList(urls, new File(Constants.Files.RUNTIME_CONFIG_JAR, Constants.Files.CLASSPATH));
    }

    addClassPathsToList(urls, new File(Constants.Files.RUNTIME_CONFIG_JAR, Constants.Files.APPLICATION_CLASSPATH));
    return urls.toArray(new URL[urls.size()]);
  }

  /**
   * Creates a {@link ClassLoader} to be used by this container that load classes from the given classpath.
   */
  private static ClassLoader createContainerClassLoader(URL[] classpath) {
    String containerClassLoaderName = System.getProperty(Constants.TWILL_CONTAINER_CLASSLOADER);
    URLClassLoader classLoader = new URLClassLoader(classpath);
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

  private static void addClassPathsToList(List<URL> urls, File classpathFile) throws IOException {
    try (BufferedReader reader = Files.newBufferedReader(classpathFile.toPath(), StandardCharsets.UTF_8)) {
      String line = reader.readLine();
      if (line != null) {
        for (String path : expand(line).split(":")) {
          urls.addAll(getClassPaths(path.trim()));
        }
      }
    }
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
}

