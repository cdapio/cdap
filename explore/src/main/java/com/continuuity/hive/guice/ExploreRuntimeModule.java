package com.continuuity.hive.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.runtime.RuntimeModule;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.inject.Module;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Guice module for explore. Also isolates Hive classes.
 */
public class ExploreRuntimeModule extends RuntimeModule {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreRuntimeModule.class);

  private final RuntimeModule hiveRunTimeModule;

  public ExploreRuntimeModule(CConfiguration cConfiguration) {
    hiveRunTimeModule = loadHiveRuntimeModule(cConfiguration);
  }

  @Override
  public Module getInMemoryModules() {
    return hiveRunTimeModule.getInMemoryModules();
  }

  @Override
  public Module getSingleNodeModules() {
    return hiveRunTimeModule.getSingleNodeModules();
  }

  @Override
  public Module getDistributedModules() {
    return hiveRunTimeModule.getDistributedModules();
  }

  static Iterable<URL> getClassPath(String hiveClassPath) {
    if (hiveClassPath == null) {
      return null;
    }

    return Iterables.transform(Splitter.on(':').split(hiveClassPath), STRING_URL_FUNCTION);
  }

  private static final Function<String, URL> STRING_URL_FUNCTION =
    new Function<String, URL>() {
      @Override
      public URL apply(String input) {
        try {
          return new File(input).toURI().toURL();
        } catch (MalformedURLException e) {
          throw Throwables.propagate(e);
        }
      }
    };

  private RuntimeModule loadHiveRuntimeModule(CConfiguration cConfiguration) {
    // HIVE_CLASSPATH will be defined in startup scripts if Hive is installed.
    String hiveClassPathStr = System.getenv(Constants.Explore.HIVE_CLASSPATH);
    LOG.debug("Hive classpath = {}", hiveClassPathStr);
    if (hiveClassPathStr == null) {
      return new MockHiveRuntimeModule();
    }

    Iterable<URL> hiveClassPath = getClassPath(hiveClassPathStr);

    String javaClassPathStr = System.getProperty("java.class.path");
    LOG.debug("Java classpath = {}", javaClassPathStr);
    Iterable<URL> javaClassPath = getClassPath(javaClassPathStr);

    ClassLoader hiveClassLoader =
      new CustomURLClassLoader(Iterables.toArray(Iterables.concat(javaClassPath, hiveClassPath), URL.class),
                         ClassLoader.getSystemClassLoader());
    return new HiveRuntimeModule(cConfiguration, hiveClassLoader);
  }
}
