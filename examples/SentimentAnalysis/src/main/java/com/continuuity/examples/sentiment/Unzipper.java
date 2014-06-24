/**
 * Copyright 2013-2014 Continuuity, Inc.
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
package com.continuuity.examples.sentiment;

import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import org.apache.ant.compress.taskdefs.Unzip;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * Unzips a zip archive.
 */
public class Unzipper {

  /**
   * Unzip an input Stream of a zip archive.
   * @param inputStream input Stream of zip archive.
   * @param destDir directory to unzip the inputStream to. The directory is created if it does not exist.
   * @throws IOException
   */
  public static void unzip(final InputStream inputStream, File destDir) throws IOException {
    // Create zip file from bytes
    File zipFile = new File("zipFile-" + System.nanoTime() + ".zip");
    Files.copy(new InputSupplier<InputStream>() {
      @Override
      public InputStream getInput() throws IOException {
        return inputStream;
      }
    }, zipFile);

    // Unzip the file created.
    if (!destDir.exists() && !destDir.mkdirs()) {
      throw new IOException("Cannot create destDir " + destDir);
    }

    Unzip unzip = new Unzip();
    unzip.setSrc(zipFile);
    unzip.setDest(destDir);
    unzip.execute();

    //noinspection ResultOfMethodCallIgnored
    zipFile.delete();
  }
}
