package com.continuuity.common.lang.jar;

import com.google.common.base.Preconditions;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Utility functions that operate on bundle jars.
 */
// TODO: remove this -- not sure how to refactor though
public class BundleJarUtil {

  private static final Logger LOG = LoggerFactory.getLogger(BundleJarUtil.class);

  public static File unpackProgramJar(Location programJarLocation, File destinationFolder) throws IOException {
    Preconditions.checkArgument(programJarLocation != null);
    Preconditions.checkArgument(programJarLocation.exists());
    Preconditions.checkArgument(destinationFolder != null);
    Preconditions.checkArgument(destinationFolder.canWrite());

    LOG.debug("Unpacking program " + programJarLocation.toURI() + " to " + destinationFolder.getAbsolutePath());
    destinationFolder.mkdirs();
    Preconditions.checkState(destinationFolder.exists());
    unJar(new ZipInputStream(programJarLocation.getInputStream()), destinationFolder);
    return destinationFolder;
  }

  private static void unJar(ZipInputStream jarInputStream, File targetDirectory) throws IOException {
    ZipEntry entry;
    while ((entry = jarInputStream.getNextEntry()) != null) {
      File output = new File(targetDirectory, entry.getName());

      if (entry.isDirectory()) {
        output.mkdirs();
      } else {
        output.getParentFile().mkdirs();

        OutputStream os = new FileOutputStream(output);
        try {
          byte[] bytes = new byte[16384];
          int bytesRead = jarInputStream.read(bytes);
          while (bytesRead != -1) {
            os.write(bytes, 0, bytesRead);
            bytesRead = jarInputStream.read(bytes);
          }
        } finally {
          os.close();
        }
      }
    }
  }

  /*

  public static URL[] locateClassPathUrls(File bundleJarFolder) throws MalformedURLException {
    return locateClassPathUrls(bundleJarFolder, DEFAULT_BUNDLE_JAR_LIB_FOLDER);
  }

  public static URL[] locateClassPathUrls(File bundleJarFolder, String libFolder) throws MalformedURLException {
    List<URL> classPathUrls = new LinkedList<URL>();
    classPathUrls.add(bundleJarFolder.toURI().toURL());
    classPathUrls.addAll(getJarURLs(new File(bundleJarFolder, libFolder)));
    URL[] classPathUrlArray = classPathUrls.toArray(new URL[classPathUrls.size()]);
    return classPathUrlArray;
  }


  private static List<URL> getJarURLs(File dir) throws MalformedURLException {
    File[] files = dir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".jar");
      }
    });
    List<URL> urls = new LinkedList<URL>();

    if (files != null) {
      for (File file : files) {
        urls.add(file.toURI().toURL());
      }
    }

    return urls;
  }
  */
}
