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
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import com.google.common.io.OutputSupplier;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Util class for common functions needed for Spark implementation.
 */
public final class SparkRuntimeUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SparkRuntimeUtils.class);

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

  /**
   * Creates a zip file which contains a serialized {@link Properties} with a given zip entry name, together with
   * all files under the given directory. This is called from Client.createConfArchive() as a workaround for the
   * SPARK-13441 bug.
   *
   * @param sparkConf the {@link SparkConf} to save
   * @param propertiesEntryName name of the zip entry for the properties
   * @param confDirPath directory to scan for files to include in the zip file
   * @param outputZipPath output file
   * @return the zip file
   */
  public static File createConfArchive(SparkConf sparkConf, final String propertiesEntryName,
                                       String confDirPath, String outputZipPath) {
    final Properties properties = new Properties();
    for (Tuple2<String, String> tuple : sparkConf.getAll()) {
      properties.put(tuple._1(), tuple._2());
    }

    try {
      File confDir = new File(confDirPath);
      final File zipFile = new File(outputZipPath);
      BundleJarUtil.createArchive(confDir, new OutputSupplier<ZipOutputStream>() {
        @Override
        public ZipOutputStream getOutput() throws IOException {
          ZipOutputStream zipOutput = new ZipOutputStream(new FileOutputStream(zipFile));
          zipOutput.putNextEntry(new ZipEntry(propertiesEntryName));
          properties.store(zipOutput, "Spark configuration.");
          zipOutput.closeEntry();

          return zipOutput;
        }
      });
      LOG.debug("Spark config archive created at {} from {}", zipFile, confDir);
      return zipFile;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private SparkRuntimeUtils() {
    // private
  }
}
