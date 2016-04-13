/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.lang.FilterClassLoader;
import co.cask.cdap.common.lang.ProgramClassLoader;

import java.io.File;

/**
 * Util class for common functions needed for Spark implementation.
 */
public final class SparkRuntimeUtils {

  // ClassLoader filter
  private static final FilterClassLoader.Filter SPARK_PROGRAM_CLASS_LOADER_FILTER = new FilterClassLoader.Filter() {

    final FilterClassLoader.Filter defaultFilter = FilterClassLoader.defaultFilter();

    @Override
    public boolean acceptResource(String resource) {
      return resource.startsWith("co/cask/cdap/api/spark/") || resource.startsWith("scala/")
        || resource.startsWith("org/apache/spark/") || resource.startsWith("akka/")
        || defaultFilter.acceptResource(resource);
    }

    @Override
    public boolean acceptPackage(String packageName) {
      if (packageName.equals("co.cask.cdap.api.spark") || packageName.startsWith("co.cask.cdap.api.spark.")) {
        return true;
      }
      if (packageName.equals("scala") || packageName.startsWith("scala.")) {
        return true;
      }
      if (packageName.equals("org.apache.spark") || packageName.startsWith("org.apache.spark.")) {
        return true;
      }
      if (packageName.equals("akka") || packageName.startsWith("akka.")) {
        return true;
      }
      return defaultFilter.acceptResource(packageName);
    }
  };

  /**
   * Creates a {@link ProgramClassLoader} that have Spark classes visible.
   */
  public static ProgramClassLoader createProgramClassLoader(CConfiguration cConf, File dir,
                                                            ClassLoader unfilteredClassLoader) {
    ClassLoader parent = new FilterClassLoader(unfilteredClassLoader, SPARK_PROGRAM_CLASS_LOADER_FILTER);
    return new ProgramClassLoader(cConf, dir, parent);
  }

  private SparkRuntimeUtils() {
    // private
  }
}
