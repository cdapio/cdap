package com.continuuity.common.lang.jar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedList;
import java.util.List;

/**
 * ClassLoader that implements bundle jar feature, in which the application jar contains
 * its dependency jars inside the "/lib" folder (by default) within the application jar.
 * <p/>
 * Useful for
 * 1) using third party jars that overwrite each other's files
 * (e.g. Datanucleus jars each have plugin.xml at same location
 * relative to the jar root, so if you package your application
 * as an uber-jar, your application jar will only contain one
 * of the plugin.xml at best unless you do some manual configuration.
 * <p/>
 * Not (yet) useful for
 * 1) avoiding classpath conflicts with Reactor's dependency jars
 * (e.g. you want to use Guava 16.0.1 but Reactor uses 13.0.1)
 */
public class ProgramClassLoader extends URLClassLoader {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramClassLoader.class);

  /**
   * Convenience class to construct a classloader for a program from an unpacked jar directory.
   * Adds <unpackedJarDir>/{.,*.jar,lib/*.jar} to the {@link URLClassLoader}.
   *
   * @param unpackedJarDir Directory of the unpacked jar to be used in the classpath.
   * @param parentDelegate Parent classloader.
   */
  public ProgramClassLoader(File unpackedJarDir, ClassLoader parentDelegate) {
    super(getClassPathUrls(unpackedJarDir), parentDelegate);
  }

  private static URL[] getClassPathUrls(File unpackedJarDir) {
    List<URL> classPathUrls = new LinkedList<URL>();

    try {
      classPathUrls.add(unpackedJarDir.toURI().toURL());
    } catch (MalformedURLException e) {
      LOG.error("Error in adding unpackedJarDir to classPathUrls", e);
    }

    try {
      classPathUrls.addAll(getJarURLs(unpackedJarDir));
    } catch (MalformedURLException e) {
      LOG.error("Error in adding jar URLs to classPathUrls", e);
    }

    try {
      classPathUrls.addAll(getJarURLs(new File(unpackedJarDir, "lib")));
    } catch (MalformedURLException e) {
      LOG.error("Error in adding jar URLs to classPathUrls", e);
    }

    return classPathUrls.toArray(new URL[classPathUrls.size()]);
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
}
