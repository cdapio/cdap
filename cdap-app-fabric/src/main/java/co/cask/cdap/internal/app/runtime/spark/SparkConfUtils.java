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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.common.conf.CConfiguration;
import scala.Tuple2;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * An isolated utility for creating a configuration zip file that the Spark runtime needed. It is a
 * workaround for Spark bug (SPARK-13441, CDAP-5019). This class will be added to the spark assembly jar
 * during rewrite of the yarn.Client class.
 *
 * IMPORTANT: This class should only use Java SDK and Scala classes and shouldn't use any inner class.
 *
 * @see {@link SparkUtils#getRewrittenSparkAssemblyJar(CConfiguration)}.
 */
public final class SparkConfUtils {

  /**
   * Creates a zip file which contains a serialized {@link Properties} with a given zip entry name, together with
   * all files under the given directory.
   *
   * @param props properties to serialize
   * @param propertiesEntryName name of the zip entry for the properties
   * @param confDir directory to scan for files to include in the zip file
   * @param outputZipFile output file
   */
  public static File createZip(Tuple2<String, String>[] props, String propertiesEntryName,
                               String confDir, String outputZipFile) {
    final Properties properties = new Properties();
    for (Tuple2<String, String> tuple : props) {
      properties.put(tuple._1(), tuple._2());
    }

    File zipFile = new File(outputZipFile);
    try (ZipOutputStream zipOutput = new ZipOutputStream(new FileOutputStream(zipFile))) {
      zipOutput.putNextEntry(new ZipEntry(propertiesEntryName));
      properties.store(zipOutput, "Spark configuration.");
      zipOutput.closeEntry();

      // Add all files under the confDir to the zip
      File dir = new File(confDir);
      URI baseURI = dir.toURI();

      File[] files = dir.listFiles();
      if (files != null) {
        for (File file : files) {
          if (file.isDirectory()) {
            continue;
          }
          zipOutput.putNextEntry(new ZipEntry(baseURI.relativize(file.toURI()).getPath()));
          Files.copy(file.toPath(), zipOutput);
          zipOutput.closeEntry();
        }
      }
      return zipFile;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private SparkConfUtils() {
    // no-op
  }
}
