package com.continuuity.common.lang;

import com.continuuity.api.app.Application;
import com.continuuity.common.internal.guava.ClassPath;
import com.continuuity.common.lang.jar.ProgramClassLoader;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import org.apache.twill.internal.utils.Dependencies;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
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
  private static final List<String> CONTINUUITY_API_PACKAGES = Lists.newArrayList("com.continuuity.api");
  private static final Predicate<URI> JAR_ONLY_URI = new Predicate<URI>() {
    @Override
    public boolean apply(URI input) {
      return input.getPath().endsWith(".jar");
    }
  };

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

  public static Iterable<String> getAPIResources(ClassLoader classLoader) throws IOException {
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

    Set<String> resources = getResources(classLoader, getAPIClassPath(), CONTINUUITY_API_PACKAGES,
                                         true, bootstrapPaths, Sets.<String>newHashSet());

    return getResources(classLoader, ClassPath.from(classLoader, JAR_ONLY_URI),
                        HADOOP_PACKAGES, false, bootstrapPaths, resources);
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

  /**
   * Gathers all resources for api classes.
   */
  private static ClassPath getAPIClassPath() throws IOException {
    ClassLoader classLoader = Application.class.getClassLoader();
    String resourceName = Application.class.getName().replace('.', '/') + ".class";
    URL url = classLoader.getResource(resourceName);
    if (url == null) {
      throw new IOException("Resource not found for " + resourceName);
    }

    try {
      URI classPathURI = getClassPathURL(resourceName, url).toURI();
      return ClassPath.from(classPathURI, classLoader);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  /**
   * Find the classpath that contains the given resource.
   */
  private static URL getClassPathURL(String resourceName, URL resourceURL) {
    try {
      if ("file".equals(resourceURL.getProtocol())) {
        String path = resourceURL.getFile();
        // Compute the directory container the class.
        int endIdx = path.length() - resourceName.length();
        if (endIdx > 1) {
          // If it is not the root directory, return the end index to remove the trailing '/'.
          endIdx--;
        }
        return new URL("file", "", -1, path.substring(0, endIdx));
      }
      if ("jar".equals(resourceURL.getProtocol())) {
        String path = resourceURL.getFile();
        return URI.create(path.substring(0, path.indexOf("!/"))).toURL();
      }
    } catch (MalformedURLException e) {
      throw Throwables.propagate(e);
    }
    throw new IllegalStateException("Unsupported class URL: " + resourceURL);
  }


  private static <T extends Collection<String>> T getResources(ClassLoader classLoader,
                                                               ClassPath classPath,
                                                               Iterable<String> packages,
                                                               boolean includeDependencies,
                                                               final Set<String> bootstrapPaths,
                                                               final T result) throws IOException {
    Set<String> classes = Sets.newHashSet();
    for (String pkg : packages) {
      ImmutableSet<ClassPath.ClassInfo> packageClasses = classPath.getAllClassesRecursive(pkg);
      for (ClassPath.ClassInfo cls : packageClasses) {
        result.add(cls.getResourceName());
        classes.add(cls.getName());
      }
    }

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
