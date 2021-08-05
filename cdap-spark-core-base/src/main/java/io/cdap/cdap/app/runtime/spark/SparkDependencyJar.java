/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark;

import com.google.common.io.Files;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeService;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.common.utils.DirUtils;
import org.apache.twill.api.TwillRunnable;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.zip.Deflater;

/**
 * Class to create a jar containing all the dependent jars needed to run cdap-spark-core classes.
 */
public class SparkDependencyJar {

  public static void main(String[] args) throws IOException, URISyntaxException {
    if (args.length < 1) {
      System.err.println("Must provide a target directory to write to");
      return;
    }
    File targetFile = new File(args[0]);
    buildDependencyJar(targetFile);
  }

  /**
   * Packages all the dependencies of the Spark job. It contains all CDAP classes that are needed to run the
   * user spark program.
   *
   * @param targetFile the target file for the jar created
   * @return list of jar file name that contains all dependency jars in sorted order
   * @throws IOException if failed to package the jar
   */
  public static Iterable<String> buildDependencyJar(File targetFile) throws IOException, URISyntaxException {
    Set<String> classpath = new TreeSet<>();
    try (JarOutputStream jarOut = new JarOutputStream(new BufferedOutputStream(new FileOutputStream(targetFile)))) {
      jarOut.setLevel(Deflater.NO_COMPRESSION);

      // Zip all the jar files under the same directory that contains the jar for this class and twill class.
      // Those are the directory created by TWILL that contains all dependency jars for this container
      Class<?> runtimeServiceClass = SparkRuntimeService.class;
      for (String className : Arrays.asList(runtimeServiceClass.getName(), TwillRunnable.class.getName())) {
        Enumeration<URL> resources =
          runtimeServiceClass.getClassLoader().getResources(className.replace('.', '/') + ".class");
        while (resources.hasMoreElements()) {
          URL classURL = resources.nextElement();
          File libDir = new File(ClassLoaders.getClassPathURL(className, classURL).toURI()).getParentFile();

          for (File file : DirUtils.listFiles(libDir, "jar")) {
            if (classpath.add(file.getName())) {
              jarOut.putNextEntry(new JarEntry(file.getName()));
              Files.copy(file, jarOut);
              jarOut.closeEntry();
            }
          }
        }
      }
    }
    return classpath;
  }
}