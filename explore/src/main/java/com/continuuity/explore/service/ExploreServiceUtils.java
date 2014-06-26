package com.continuuity.explore.service;

import com.continuuity.data2.datafabric.dataset.service.DatasetService;
import com.continuuity.data2.util.hbase.HBaseTableUtilFactory;
import com.continuuity.explore.guice.ExploreRuntimeModule;
import com.continuuity.explore.service.hive.Hive12ExploreService;
import com.continuuity.explore.service.hive.Hive13ExploreService;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.twill.internal.utils.Dependencies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Utility class for the explore service.
 */
public class ExploreServiceUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreServiceUtils.class);
  /**
   * Hive support enum.
   */
  public enum HiveSuport {
    // todo populate this with whatever hive version CDH4.3 runs with - REACTOR-229
    HIVE_12(Hive12ExploreService.class),
    HIVE_13(Hive13ExploreService.class);

    private Class hiveExploreServiceClass;

    private HiveSuport(Class hiveExploreServiceClass) {
      this.hiveExploreServiceClass = hiveExploreServiceClass;
    }

    public Class getHiveExploreServiceClass() {
      return hiveExploreServiceClass;
    }
  }

  // Caching the dependencies so that we don't trace them twice
  private static Set<File> exploreDependencies = null;

  public static Iterable<File> getClassPathJarsFiles(String hiveClassPath) {
    if (hiveClassPath == null) {
      return null;
    }
    return Iterables.transform(Splitter.on(':').split(hiveClassPath), STRING_FILE_FUNCTION);
  }

  private static final Function<String, File> STRING_FILE_FUNCTION =
    new Function<String, File>() {
      @Override
      public File apply(String input) {
        return new File(input).getAbsoluteFile();
      }
    };

  /**
   * Builds a class loader with the class path provided.
   */
  public static ClassLoader buildClassLoader(String classPathStr) {
    Iterable<File> hiveClassPath = getClassPathJarsFiles(classPathStr);
    ImmutableList.Builder<URL> builder = ImmutableList.builder();
    for (File jar : hiveClassPath) {
      try {
        builder.add(jar.toURI().toURL());
      } catch (MalformedURLException e) {
        LOG.error("Jar URL is malformed", e);
        Throwables.propagate(e);
      }
    }
    return new URLClassLoader(Iterables.toArray(builder.build(), URL.class),
                              ClassLoader.getSystemClassLoader());
  }

  public static Class getHiveService() {
    HiveSuport hiveVersion = checkHiveSupport(null);
    Class hiveServiceCl = hiveVersion.getHiveExploreServiceClass();
    return hiveServiceCl;
  }

  /**
   * Check that Hive is in the class path - with a right version.
   *
   * @param hiveClassLoader class loader to use to load hive classes.
   *                        If null, the class loader of this class is used.
   */
  public static HiveSuport checkHiveSupport(ClassLoader hiveClassLoader) {
    try {
      ClassLoader usingCL = hiveClassLoader;
      if (usingCL == null) {
        usingCL = ExploreServiceUtils.class.getClassLoader();
      }

      // In Hive 12, CLIService.getOperationStatus returns OperationState.
      // In Hive 13, CLIService.getOperationStatus returns OperationStatus.
      Class cliServiceClass = usingCL.loadClass("org.apache.hive.service.cli.CLIService");
      Class operationHandleCl = usingCL.loadClass("org.apache.hive.service.cli.OperationHandle");
      Method getOperationMethod = cliServiceClass.getDeclaredMethod("getOperationStatus", operationHandleCl);

      // Rowset is an interface in Hive 13, but a class in Hive 12
      Class rowSetClass = usingCL.loadClass("org.apache.hive.service.cli.RowSet");

      if (rowSetClass.isInterface()
        && getOperationMethod.getReturnType() == usingCL.loadClass("org.apache.hive.service.cli.OperationStatus")) {
        return HiveSuport.HIVE_13;
      } else if (!rowSetClass.isInterface()
        && getOperationMethod.getReturnType() == usingCL.loadClass("org.apache.hive.service.cli.OperationState")) {
        return HiveSuport.HIVE_12;
      }
      throw new RuntimeException("Hive distribution not supported.");
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      throw new RuntimeException("Hive jars not present in classpath", e);
    }
  }

  /**
   * Return the list of absolute paths of the bootstrap classes.
   */
  public static Set<String> getBoostrapClasses() {
    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    for (String classpath : Splitter.on(File.pathSeparatorChar).split(System.getProperty("sun.boot.class.path"))) {
      File file = new File(classpath);
      builder.add(file.getAbsolutePath());
      try {
        builder.add(file.getCanonicalPath());
      } catch (IOException e) {
        LOG.warn("Could not add canonical path to aux class path for file {}", file.toString(), e);
      }
    }
    return builder.build();
  }

  /**
   * Trace the jar dependencies needed by the Explore container, using
   * a class loader will be built with the class path given in parameter.
   *
   * @param hiveClassPathStr Class path to use to build the custom class loader.
   * @return an ordered set of jar files.
   */
  public static Set<File> traceExploreDependencies(String hiveClassPathStr) throws IOException {
    if (exploreDependencies != null) {
      return exploreDependencies;
    }

    ClassLoader classLoader = buildClassLoader(hiveClassPathStr);
    return traceExploreDependencies(classLoader);
  }

  /**
   * Trace the jar dependencies needed by the Explore container.
   *
   * @param classLoader class loader to use to trace the dependencies.
   *                    If it is null, use the class loader of this class.
   * @return an ordered set of jar files.
   */
  public static Set<File> traceExploreDependencies(ClassLoader classLoader)
    throws IOException {
    if (exploreDependencies != null) {
      return exploreDependencies;
    }

    ClassLoader usingCL = classLoader;
    if (classLoader == null) {
      usingCL = ExploreRuntimeModule.class.getClassLoader();
    }
    Set<String> bootstrapClassPaths = getBoostrapClasses();

    Set<File> hBaseTableDeps = traceDependencies(new HBaseTableUtilFactory().get().getClass().getCanonicalName(),
                                                 bootstrapClassPaths, usingCL);

    // Note the order of dependency jars is important so that HBase jars come first in the classpath order
    // LinkedHashSet maintains insertion order while removing duplicate entries.
    Set<File> orderedDependencies = new LinkedHashSet<File>();
    orderedDependencies.addAll(hBaseTableDeps);
    orderedDependencies.addAll(traceDependencies(DatasetService.class.getCanonicalName(),
                                                 bootstrapClassPaths, usingCL));
    orderedDependencies.addAll(traceDependencies("com.continuuity.hive.datasets.DatasetStorageHandler",
                                                 bootstrapClassPaths, usingCL));
    orderedDependencies.addAll(traceDependencies("org.apache.hadoop.hive.ql.exec.mr.ExecDriver",
                                                 bootstrapClassPaths, usingCL));
    orderedDependencies.addAll(traceDependencies("org.apache.hive.service.cli.CLIService",
                                                 bootstrapClassPaths, usingCL));
    orderedDependencies.addAll(traceDependencies("org.apache.hadoop.mapred.YarnClientProtocolProvider",
                                                 bootstrapClassPaths, usingCL));
    exploreDependencies = orderedDependencies;
    return orderedDependencies;
  }

  /**
   * Trace the dependencies files of the given className, using the classLoader, and excluding any class contained in
   * the bootstrapClassPaths.
   */
  public static Set<File> traceDependencies(String className, final Set<String> bootstrapClassPaths,
                                            ClassLoader classLoader)
    throws IOException {
    ClassLoader usingCL = classLoader;
    if (usingCL == null) {
      usingCL = ExploreRuntimeModule.class.getClassLoader();
    }
    final Set<File> jarFiles = Sets.newHashSet();

    Dependencies.findClassDependencies(
      usingCL,
      new Dependencies.ClassAcceptor() {
        @Override
        public boolean accept(String className, URL classUrl, URL classPathUrl) {
          if (bootstrapClassPaths.contains(classPathUrl.getFile())) {
            return false;
          }

          jarFiles.add(new File(classPathUrl.getFile()));
          return true;
        }
      },
      className
    );

    return jarFiles;
  }


}
