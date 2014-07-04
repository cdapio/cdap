package com.continuuity.common.lang;

import com.continuuity.common.internal.guava.ClassPath;
import com.continuuity.common.lang.jar.ProgramClassLoader;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import org.apache.commons.io.FileUtils;
import org.apache.twill.internal.utils.Dependencies;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Utility class for collection of methods for dealing with ClassLoader and loading class.
 */
public final class ClassLoaders {

  private static final List<String> HADOOP_PACKAGES = Lists.newArrayList("org.apache.hadoop");
  private static final List<String> CONTINUUITY_API_PACKAGES = Lists.newArrayList(
    "com.continuuity.api"/*, "com.continuuity.data2", "com.continuuity.internal"*/
  );

  private ClassLoaders() { }

  public static ProgramClassLoader newProgramClassLoader(File unpackedJarDir, Iterable<String> apiResourceList,
                                                         ClassLoader parentClassLoader) throws IOException {

    Predicate<String> predicate = Predicates.in(Sets.newHashSet(apiResourceList));
    ClassLoader filterClassLoader = new FilterClassLoader(predicate, parentClassLoader);
    return new ProgramClassLoader(unpackedJarDir, filterClassLoader);
  }

  public static ProgramClassLoader newProgramClassLoaderWithoutFilter(
    File unpackedJarDir, ClassLoader parentClassLoader) throws IOException {

    return new ProgramClassLoader(unpackedJarDir, parentClassLoader);
  }

  public static Iterable<String> getApiResourceList(ClassLoader classLoader) throws IOException {
    // Get the bootstrap classpath. This is for exclusion.
    Set<String> bootstrapPaths = Sets.newHashSet();
    for (String classpath : Splitter.on(File.pathSeparatorChar).split(System.getProperty("sun.boot.class.path"))) {
      File file = new File(classpath);
      bootstrapPaths.add(file.getAbsolutePath());
      try {
        bootstrapPaths.add(file.getCanonicalPath());
      } catch (IOException e) {
        // Ignore the exception and proceed.
      }
    }

    Set<String> resources = getResources(classLoader, CONTINUUITY_API_PACKAGES, true,
                                         bootstrapPaths, Sets.<String>newHashSet());
    return getResources(classLoader, HADOOP_PACKAGES, false, bootstrapPaths, resources);
  }


  /**
   * Returns the ClassLoader of the given type. If the given type is a {@link ParameterizedType}, it returns
   * a {@link CombineClassLoader} of all types. The context ClassLoader or System ClassLoader would be used as
   * the parent of the CombineClassLoader.
   *
   * @return A new CombineClassLoader. If no ClassLoader is found from the type,
   *         it returns the current thread context ClassLoader if it's not null, otherwise, return system ClassLoader.
   */
  public static ClassLoader getClassLoader(TypeToken<?> type) {
    Set<ClassLoader> classLoaders = Sets.newIdentityHashSet();

    // Breath first traversal into the Type.
    Queue<TypeToken<?>> queue = Lists.newLinkedList();
    queue.add(type);
    while (!queue.isEmpty()) {
      type = queue.remove();
      ClassLoader classLoader = type.getRawType().getClassLoader();
      if (classLoader != null) {
        classLoaders.add(classLoader);
      }

      if (type.getType() instanceof ParameterizedType) {
        for (Type typeArg : ((ParameterizedType) type.getType()).getActualTypeArguments()) {
          queue.add(TypeToken.of(typeArg));
        }
      }
    }

    // Determine the parent classloader
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    ClassLoader parent = (contextClassLoader == null) ? ClassLoader.getSystemClassLoader() : contextClassLoader;

    if (classLoaders.isEmpty()) {
      return parent;
    }
    return new CombineClassLoader(parent, classLoaders);
  }

  private static <T extends Collection<String>> T getResources(ClassLoader classLoader,
                                                               Iterable<String> packages,
                                                               boolean includeDependencies,
                                                               final Set<String> bootstrapPaths,
                                                               final T result) throws IOException {

    Set<String> classes = Sets.newHashSet();
    final ClassPath classPath = ClassPath.from(classLoader);
    for (String pkg : packages) {
      ImmutableSet<ClassPath.ClassInfo> packageClasses = classPath.getAllClassesRecursive(pkg);
      for (ClassPath.ClassInfo cls : packageClasses) {
        result.add(cls.getResourceName());
        classes.add(cls.getName());
      }
    }

//    FileUtils.writeLines(new File("classloaders." + packages.iterator().next() + ".log"), classes);

    if (includeDependencies) {
      final Set<URL> classPathSeen = Sets.newHashSet();

      Dependencies.findClassDependencies(classLoader, new Dependencies.ClassAcceptor() {
        @Override
        public boolean accept(String className, URL classUrl, URL classPathUrl) {
          if (bootstrapPaths.contains(classPathUrl.getFile())) {
            return false;
          }

          // Add all resources in the given class path
          if (!classPathSeen.add(classPathUrl)) {
            return true;
          }

          try {
            ClassPath classPath = ClassPath.from(classPathUrl.toURI(), ClassLoader.getSystemClassLoader());
            for (ClassPath.ResourceInfo resourceInfo : classPath.getResources()) {
              result.add(resourceInfo.getResourceName());
            }
          } catch (Exception e) {
            // If fail to get classes/resources from the classpath, ignore this classpath.
          }
          return true;
        }
      }, classes);
    }

    return result;
  }

  /**
   * Loads the class with the given class name with the given classloader. If it is {@code null},
   * load the class with the context ClassLoader of current thread if it presents, otherwise load the class
   * with the ClassLoader of the caller object.
   *
   * @param className Name of the class to load.
   * @param classLoader Classloader for loading the class. It could be {@code null}.
   * @param caller The object who call this method.
   * @return The loaded class.
   * @throws ClassNotFoundException If failed to load the given class.
   */
  public static Class<?> loadClass(String className, @Nullable ClassLoader classLoader,
                                   Object caller) throws ClassNotFoundException {
    ClassLoader cl = Objects.firstNonNull(classLoader,
                                          Objects.firstNonNull(Thread.currentThread().getContextClassLoader(),
                                                               caller.getClass().getClassLoader()));
    return cl.loadClass(className);
  }
}
