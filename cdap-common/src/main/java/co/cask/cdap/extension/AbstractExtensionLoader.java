/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.extension;

import co.cask.cdap.common.utils.DirUtils;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * A class to load extensions for supported extension types from a configured extensions directory. It uses the Java
 * {@link ServiceLoader} architecture to load extensions from the specified directory. It first tries to load
 * extensions using a {@link URLClassLoader} consisting of all jars in the configured extensions directory. For unit
 * tests, it loads extensions using the system classloader. If an extension is not found via the extensions classloader
 * or the system classloader, it returns a default extension.
 *
 * The supported directory structure is:
 * <pre>
 *   [extensions-directory]/[extensions-module-directory-1]/
 *   [extensions-directory]/[extensions-module-directory-1]/file-containing-extensions-1.jar
 *   [extensions-directory]/[extensions-module-directory-1]/file-containing-dependencies-1.jar
 *   [extensions-directory]/[extensions-module-directory-1]/file-containing-dependencies-2.jar
 *   [extensions-directory]/[extensions-module-directory-2]/
 *   [extensions-directory]/[extensions-module-directory-2]/file-containing-extensions-2.jar
 *   [extensions-directory]/[extensions-module-directory-2]/file-containing-extensions-3.jar
 *   [extensions-directory]/[extensions-module-directory-2]/file-containing-dependencies-3.jar
 *   [extensions-directory]/[extensions-module-directory-2]/file-containing-dependencies-4.jar
 *   ...
 *   [extensions-directory]/[extensions-module-directory-n]/file-containing-extensions-n.jar
 * </pre>
 *
 * Each extensions jar file above can contain multiple extensions.
 *
 * @param <EXTENSION_TYPE> the data type of the objects that a given extension can be used for. e.g. for a program
 *                        runtime extension, the list of program types that the extension can be used for
 * @param <EXTENSION> the data type of the extension
 */
public abstract class AbstractExtensionLoader<EXTENSION_TYPE, EXTENSION> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractExtensionLoader.class);

  private final List<String> extDirs;
  private final Class<EXTENSION> extensionClass;
  // A ServiceLoader that loads extension implementation from the CDAP system classloader.
  private final ServiceLoader<EXTENSION> systemExtensionLoader;
  private final LoadingCache<EXTENSION_TYPE, AtomicReference<EXTENSION>> extensionsCache;
  private final LoadingCache<File, ServiceLoader<EXTENSION>> serviceLoaderCache;
  private Map<EXTENSION_TYPE, EXTENSION> allExtensions;

  @SuppressWarnings("unchecked")
  public AbstractExtensionLoader(String extDirs) {
    this.extDirs = ImmutableList.copyOf(Splitter.on(';').omitEmptyStrings().trimResults().split(extDirs));
    Type type = TypeToken.of(getClass()).getSupertype(AbstractExtensionLoader.class).getType();
    // type should always be an instance of ParameterizedType
    Preconditions.checkState(type instanceof ParameterizedType);
    Type extensionType = ((ParameterizedType) type).getActualTypeArguments()[1];
    // extensionType should always be an instance of Class
    Preconditions.checkState(extensionType instanceof Class);
    this.extensionClass = (Class<EXTENSION>) extensionType;
    this.systemExtensionLoader = ServiceLoader.load(this.extensionClass);
    this.serviceLoaderCache = createServiceLoaderCache();
    this.extensionsCache = createExtensionsCache();
  }

  /**
   * Returns the extension for the specified object if one is found, otherwise returns {@code null}.
   */
  @Nullable
  public EXTENSION get(EXTENSION_TYPE type) {
    return extensionsCache.getUnchecked(type).get();
  }

  /**
   * Returns all the extensions from the extension directory.
   *
   * @return map of extension type to the first extension that supports the extension type
   */
  public synchronized Map<EXTENSION_TYPE, EXTENSION> getAll() {
    if (allExtensions != null) {
      return allExtensions;
    }

    Map<EXTENSION_TYPE, EXTENSION> result = new HashMap<>();

    for (String dir : extDirs) {
      File extDir = new File(dir);
      if (!extDir.isDirectory()) {
        continue;
      }

      // Each module would be under a directory of the extension directory
      List<File> files = new ArrayList<>(DirUtils.listFiles(extDir));
      Collections.sort(files);
      for (File moduleDir : files) {
        if (!moduleDir.isDirectory()) {
          continue;
        }
        // Try to find a provider that can support the given program type.
        try {
          putEntriesIfAbsent(result, getAllExtensions(serviceLoaderCache.getUnchecked(moduleDir)));
        } catch (Exception e) {
          LOG.warn("Exception raised when loading an extension from {}. Extension ignored.", moduleDir, e);
        }
      }
    }

    // Also put everything from system classloader
    putEntriesIfAbsent(result, getAllExtensions(systemExtensionLoader));

    allExtensions = result;
    return allExtensions;
  }

  /**
   * Returns the set of objects that the extension supports. Implementations should return the set of objects of type
   * #EXTENSION_TYPE that the specified extension applies to. A given extension can then be loaded for the specified
   * type by using the #getExtension method.
   *
   * @param extension the extension for which supported types are requested
   * @return the set of objects that the specified extension supports.
   */
  protected abstract Set<EXTENSION_TYPE> getSupportedTypesForProvider(EXTENSION extension);

  private void putEntriesIfAbsent(Map<EXTENSION_TYPE, EXTENSION> result, Map<EXTENSION_TYPE, EXTENSION> entries) {
    for (Map.Entry<EXTENSION_TYPE, EXTENSION> entry : entries.entrySet()) {
      if (!result.containsKey(entry.getKey())) {
        result.put(entry.getKey(), entry.getValue());
      }
    }
  }

  private LoadingCache<EXTENSION_TYPE, AtomicReference<EXTENSION>> createExtensionsCache() {
    return CacheBuilder.newBuilder().build(new CacheLoader<EXTENSION_TYPE, AtomicReference<EXTENSION>>() {
      @Override
      public AtomicReference<EXTENSION> load(EXTENSION_TYPE extensionType) throws Exception {
        EXTENSION extension = null;
        try {
          extension = findExtension(extensionType);
        } catch (Throwable t) {
          LOG.warn("Failed to load extension for type {}.", extensionType);
        }
        return new AtomicReference<>(extension);
      }
    });
  }

  /**
   * Finds the first extension from the given {@link ServiceLoader} that supports the specified key.
   */
  @Nullable
  private EXTENSION findExtension(EXTENSION_TYPE type) {
    for (String dir : extDirs) {
      File extDir = new File(dir);
      if (!extDir.isDirectory()) {
        continue;
      }

      // Each module would be under a directory of the extension directory
      List<File> files = new ArrayList<>(DirUtils.listFiles(extDir));
      Collections.sort(files);
      for (File moduleDir : files) {
        if (!moduleDir.isDirectory()) {
          continue;
        }
        // Try to find a provider that can support the given program type.
        EXTENSION extension = getAllExtensions(serviceLoaderCache.getUnchecked(moduleDir)).get(type);
        if (extension != null) {
          return extension;
        }
      }
    }
    // For unit tests, try to load the extensions from the system classloader. This is because in unit tests,
    // extensions are part of the test dependency, hence in the unit-test ClassLoader.
    return getAllExtensions(systemExtensionLoader).get(type);
  }

  /**
   * Returns all the extensions in the extensions directory using the specified {@link ServiceLoader}.
   */
  private Map<EXTENSION_TYPE, EXTENSION> getAllExtensions(ServiceLoader<EXTENSION> serviceLoader) {
    Map<EXTENSION_TYPE, EXTENSION> extensions = new HashMap<>();
    Iterator<EXTENSION> iterator = serviceLoader.iterator();
    // Cannot use for each loop here, because we want to catch exceptions during iterator.next().
    //noinspection WhileLoopReplaceableByForEach
    while (iterator.hasNext()) {
      try {
        EXTENSION extension = iterator.next();
        for (EXTENSION_TYPE type : getSupportedTypesForProvider(extension)) {
          if (extensions.containsKey(type)) {
            LOG.info("Ignoring extension {} for type {}", extension, type);
          } else {
            extensions.put(type, extension);
          }
        }
      } catch (Throwable t) {
        // Need to catch Throwable because ServiceLoader throws ServiceConfigurationError (which is an Error) if the
        // extension can not be loaded successfully.
        LOG.warn("Error while loading extension. Extension will be ignored.", t);
      }
    }
    return extensions;
  }

  /**
   * Creates a cache for caching extension directory to {@link ServiceLoader} of {@link EXTENSION}.
   */
  private LoadingCache<File, ServiceLoader<EXTENSION>> createServiceLoaderCache() {
    return CacheBuilder.newBuilder().build(new CacheLoader<File, ServiceLoader<EXTENSION>>() {
      @Override
      public ServiceLoader<EXTENSION> load(File dir) throws Exception {
        return createServiceLoader(dir);
      }
    });
  }

  /**
   * Creates a {@link ServiceLoader} from the {@link ClassLoader} created by all jar files under the given directory.
   */
  private ServiceLoader<EXTENSION> createServiceLoader(File dir) {
    List<File> files = new ArrayList<>(DirUtils.listFiles(dir, "jar"));
    Collections.sort(files);

    URL[] urls = Iterables.toArray(Iterables.transform(files, new Function<File, URL>() {
      @Override
      public URL apply(File input) {
        try {
          return input.toURI().toURL();
        } catch (MalformedURLException e) {
          // Shouldn't happen
          throw Throwables.propagate(e);
        }
      }
    }), URL.class);

    URLClassLoader classLoader = new URLClassLoader(urls, getClass().getClassLoader());
    return ServiceLoader.load(extensionClass, classLoader);
  }
}
