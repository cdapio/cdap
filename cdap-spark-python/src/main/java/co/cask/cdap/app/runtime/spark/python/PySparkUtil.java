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

package co.cask.cdap.app.runtime.spark.python;

import co.cask.cdap.common.lang.jar.BundleJarUtil;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.zip.ZipOutputStream;

/**
 * Helper class for PySpark execution.
 *
 * This class is intentionally written in this module and used by SparkRuntimeService, so that this module
 * jar is always getting included in distributed cache via dependency tracing.
 */
public final class PySparkUtil {

  /**
   * Create an archive file that contains all CDAP PySpark libraries.
   */
  public static File createPySparkLib(File tempDir) throws IOException, URISyntaxException {
    // Find the python/cdap/pyspark/__init__.py file
    URL initPyURL = PySparkUtil.class.getClassLoader().getResource("cdap/pyspark/__init__.py");
    if (initPyURL == null) {
      // This can't happen since we package the library
      throw new IOException("Missing CDAP cdap/pyspark/__init__.py from classloader");
    }

    // If the scripts come from jar already, just return the jar file.
    // This is the case in normal CDAP distribution.
    if ("jar".equals(initPyURL.getProtocol())) {
      // A JAR URL is in the format of `jar:file:///jarpath!/pathtoentry`
      return new File(URI.create(initPyURL.getPath().substring(0, initPyURL.getPath().indexOf("!/"))));
    }

    // If the python script comes from file, create a zip file that has everything under it
    File libFile = new File(tempDir, "cdap-pyspark-lib.zip");
    try (ZipOutputStream output = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(libFile)))) {
      // Directory pointing to "cdap/".
      File basePathDir = new File(initPyURL.toURI()).getParentFile().getParentFile();
      BundleJarUtil.addToArchive(basePathDir, true, output);
    }

    return libFile;
  }
}
