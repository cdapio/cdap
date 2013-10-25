package com.continuuity;

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
   * Unzip an input stream of a zip archive.
   * @param inputStream input stream of zip archive.
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
