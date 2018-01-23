/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.hive.stream;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class HiveClassLoader {

  public static ClassLoader getCL(ClassLoader currentContextClassLoader) {
    final ClassLoader bootstrapCL = new URLClassLoader(new URL[0], ClassLoader.getSystemClassLoader().getParent());
    ClassLoader parent = new ClassLoader(currentContextClassLoader) {
      @Override
      protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        try {
          return bootstrapCL.loadClass(name);
        } catch (ClassNotFoundException e) {
          if (name.startsWith("com.google.")) {
            throw new ClassNotFoundException("Class " + name + " rejected.");
          }
          return super.loadClass(name, resolve);
        }
      }
    };
    return new URLClassLoader(getURLs(), parent);
  }

  private static URL[] getURLs2() {
    try {
      List<String> fileNames = new ArrayList<>();
      fileNames.add("/opt/cdap/master/lib/co.cask.cdap.cdap-proto-4.3.3-SNAPSHOT.jar");
      fileNames.add("/opt/cdap/master/lib/cdap-master-4.3.1.jar");
      fileNames.add("/opt/cdap/master/lib/cdap-explore-4.3.1.jar");
      fileNames.add("/opt/cdap/master/lib/co.cask.cdap.cdap-common-4.3.3-SNAPSHOT.jar");
      fileNames.add("/opt/cdap/master/lib/co.cask.cdap.cdap-api-common-4.3.3-SNAPSHOT.jar");

      List<URL> fileURLs = new ArrayList<>();
      for (String fileName : fileNames) {
        File file = new File(fileName);
        if (!file.exists()) {
          throw new IllegalArgumentException(fileName);
        }
        fileURLs.add(file.toURI().toURL());
      }


      return fileURLs.toArray(new URL[fileURLs.size()]);
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  private static URL[] getURLs() {
    try {
      List<URL> fileURLs = new ArrayList<>();
      File masterLibDir = new File("/opt/cdap/master/lib");
      for (File file : masterLibDir.listFiles()) {
        if (!file.exists()) {
          throw new IllegalArgumentException(file.getAbsolutePath());
        }
        fileURLs.add(file.toURI().toURL());
      }
      File hbaseCompatLibDir = new File("/opt/cdap/hbase-compat-1.1/lib/");
      for (File file : hbaseCompatLibDir.listFiles()) {
        if (!file.exists()) {
          throw new IllegalArgumentException(file.getAbsolutePath());
        }
        fileURLs.add(file.toURI().toURL());
      }

      return fileURLs.toArray(new URL[fileURLs.size()]);
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

}
