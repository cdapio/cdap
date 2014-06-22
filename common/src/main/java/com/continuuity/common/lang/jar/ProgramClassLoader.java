package com.continuuity.common.lang.jar;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;

/**
 * ClassLoader that implements bundle jar feature, in which the application jar contains
 * its dependency jars inside the "/lib" folder (by default) within the application jar.
 *
 * Useful for
 * 1) using third party jars that overwrite each other's files
 *    (e.g. Datanucleus jars each have plugin.xml at same location
 *    relative to the jar root, so if you package your application
 *    as an uber-jar, your application jar will only contain one
 *    of the plugin.xml at best unless you do some manual configuration.
 *
 * Not (yet) useful for
 * 1) avoiding classpath conflicts with Reactor's dependency jars
 *    (e.g. you want to use Guava 16.0.1 but Reactor uses 13.0.1)
 */
public class ProgramClassLoader extends URLClassLoader {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramClassLoader.class);

  private final ClassLoader parentDelegate;
  private final boolean normalOrder;

  // TODO: Consider consolidating with MultiClassLoader.CLASS_PREFER_EXEMPTIONS
  private static final String[] CLASS_PREFER_EXEMPTIONS = new String[] {
    // Java standard library:
    "com.sun.",
    "launcher.",
    "java.",
    "javax.",
    "org.ietf",
    "org.omg",
    "org.w3c",
    "org.xml",
    "sunw.",
    // logging
    "org.slf4j",
    "com.hadoop",
    // Hadoop/HBase/ZK:
    "org.apache.hadoop",
    "org.apache.zookeeper",
    // Continuuity
    "com.continuuity",
    // Guava
    "com.google.common",
  };

  private static final String[] CLASS_PREFER_LIST = new String[] {
    "org.apache.hadoop.hive"//,
    // "com.continuuity.examples"
  };

  public ProgramClassLoader(File unpackedJarDir, ClassLoader parentDelegate, boolean normalOrder) {
    super(getClassPathUrls(unpackedJarDir), normalOrder ? determineParentClassLoader(parentDelegate) : null);
    this.parentDelegate = determineParentClassLoader(parentDelegate);
    this.normalOrder = normalOrder;
  }

  /**
   * Same as URLClassLoader, except tell URLClassLoader to use the bootstrap class loader
   * as the parent instead of the specified parent classloader. This is done so that we
   * can look for classes in the URLClassLoader first, and then look for classes in the
   * parent classloader after. The default behavior for URLClassLoader is to look in the parent
   * class loader first.
   *
   * @param unpackedJarDir Directory of the unpacked jar to be used in the classpath.
   * @param parentDelegate Parent classloader.
   */
  public ProgramClassLoader(File unpackedJarDir, ClassLoader parentDelegate) {
    this(unpackedJarDir, parentDelegate, false);
  }

  /**
   * Use a default parent classloader to instantiate this.
   *
   * @param unpackedJarDir Directory of the unpacked jar to be used in the classpath.
   */
  public ProgramClassLoader(File unpackedJarDir) {
    this(unpackedJarDir, determineParentClassLoader(null));
  }

  private static ClassLoader determineParentClassLoader(ClassLoader parentClassLoader) {
    if (parentClassLoader != null) {
      return parentClassLoader;
    }

    return Thread.currentThread().getContextClassLoader() == null ?
      ProgramClassLoader.class.getClassLoader() : Thread.currentThread().getContextClassLoader();
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

  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    if (normalOrder) {
      return super.loadClass(name, resolve);
    }
    Class<?> cls = doLoadClass(name, resolve);
    return cls;
  }

  @Override
  public URL findResource(String name) {
    if (normalOrder) {
      return super.findResource(name);
    }
    // TODO: check isPreferred() somehow to determine which order
    URL url = parentDelegate.getResource(name);
    if (url == null) {
      return super.findResource(name);
    }
    return url;
  }

  @Override
  public Enumeration<URL> findResources(String name) throws IOException {
    if (normalOrder) {
      return super.findResources(name);
    }

    List<URL> urls = Lists.newArrayList();
    Iterators.addAll(urls, Iterators.forEnumeration(parentDelegate.getResources(name)));
    Iterators.addAll(urls, Iterators.forEnumeration(super.findResources(name)));
    return Iterators.asEnumeration(urls.iterator());
  }

  private synchronized Class<?> doLoadClass(String name, boolean resolve) throws ClassNotFoundException {
    Class<?> loadedClass = findLoadedClass(name);
    if (loadedClass != null) {
      return loadedClass;
    }

    if (isPreferred(name)) {
      try {
        return super.loadClass(name, resolve);
      } catch (ClassNotFoundException e) {
        return loadClassFromParentDelegate(name, resolve);
      }
    } else {
      try {
        return loadClassFromParentDelegate(name, resolve);
      } catch (ClassNotFoundException e) {
        return super.loadClass(name, resolve);
      }
    }
  }

  private boolean isPreferred(String className) {
    for (String prefix : CLASS_PREFER_LIST) {
      if (className.startsWith(prefix)) {
        return true;
      }
    }

    for (String prefix : CLASS_PREFER_EXEMPTIONS) {
      if (className.startsWith(prefix)) {
        return false;
      }
    }
    return true;
  }

  private Class<?> loadClassFromParentDelegate(String name, boolean resolve) throws ClassNotFoundException {
    Class<?> result = parentDelegate.loadClass(name);
    if (resolve) {
      this.resolveClass(result);
    }
    return result;
  }
}
