/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.extension;

import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.FilterClassLoader;

import java.net.URL;

/**
 * Filters scala resources. This includes any classes that start with 'scala', and any resources from the scala-library
 * jar. This is used as the parent of extensions classloaders to allow different extensions to package different
 * versions of scala than the scala packaged with CDAP. For example, Spark1 uses scala 2.10 (same as CDAP),
 * but Spark2 uses 2.11.
 */
public class ScalaFilterClassLoader extends ClassLoader {

  public ScalaFilterClassLoader(ClassLoader parent) {
    super(new FilterClassLoader(parent, new FilterClassLoader.Filter() {
      @Override
      public boolean acceptResource(String resource) {
        return !resource.startsWith("scala/") && !"scala.class".equals(resource);
      }

      @Override
      public boolean acceptPackage(String packageName) {
        return !packageName.startsWith("scala/");
      }
    }));
  }

  @Override
  public URL getResource(String name) {
    URL resource = super.getResource(name);
    if (resource == null) {
      return null;
    }
    // resource = jar:file:/path/to/cdap/lib/org.scala-lang.scala-library-2.10.4.jar!/library.properties
    // baseClasspath = /path/to/cdap/lib/org.scala-lang.scala-library-2.10.4.jar
    String baseClasspath = ClassLoaders.getClassPathURL(name, resource).getPath();
    String jarName = baseClasspath.substring(baseClasspath.lastIndexOf('/') + 1, baseClasspath.length());
    return jarName.startsWith("org.scala-lang") ? null : resource;
  }
}
