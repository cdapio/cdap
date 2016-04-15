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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.app.guice.DefaultProgramRunnerFactory;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.runtime.ProgramRuntimeProvider;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.proto.ProgramType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import javax.annotation.Nullable;

/**
 * A singleton class for discovering {@link ProgramRuntimeProvider} through the runtime extension mechanism that uses
 * the Java {@link ServiceLoader} architecture.
 */
@Singleton
public class ProgramRuntimeProviderLoader {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramRuntimeProviderLoader.class);

  // The ServiceLoader that loads ProgramRunnerProvider implementation from the CDAP system classloader.
  private static final ServiceLoader<ProgramRuntimeProvider> SYSTEM_PROGRAM_RUNNER_PROVIDER_LOADER
    = ServiceLoader.load(ProgramRuntimeProvider.class);

  // The ProgramRunnerProvider serves as a tagging instance to indicate there is not
  // provider supported for a given program type
  private static final ProgramRuntimeProvider NOT_SUPPORTED_PROVIDER = new ProgramRuntimeProvider() {
    @Override
    public ProgramRunner createProgramRunner(ProgramType type, Mode mode, Injector injector) {
      throw new UnsupportedOperationException();
    }
  };

  private final LoadingCache<ProgramType, ProgramRuntimeProvider> programRunnerProviderCache;

  @VisibleForTesting
  @Inject
  public ProgramRuntimeProviderLoader(CConfiguration cConf) {
    this.programRunnerProviderCache = createProgramRunnerProviderCache(cConf);
  }

  /**
   * Returns a {@link ProgramRuntimeProvider} if one is found for the given {@link ProgramType};
   * otherwise {@code null} will be returned.
   */
  @Nullable
  public ProgramRuntimeProvider get(ProgramType programType) {
    try {
      ProgramRuntimeProvider provider = programRunnerProviderCache.get(programType);
      if (provider != NOT_SUPPORTED_PROVIDER) {
        return provider;
      }
    } catch (Throwable t) {
      LOG.warn("Failed to load ProgramRunnerProvider for {} program.", programType, t);
    }
    return null;
  }

  /**
   * Creates a cache for caching {@link ProgramRuntimeProvider} for different {@link ProgramType}.
   */
  private LoadingCache<ProgramType, ProgramRuntimeProvider> createProgramRunnerProviderCache(CConfiguration cConf) {
    // A LoadingCache from extension directory to ServiceLoader
    final LoadingCache<File, ServiceLoader<ProgramRuntimeProvider>> serviceLoaderCache = createServiceLoaderCache();

    // List of extension directories to scan
    String extDirs = cConf.get(Constants.AppFabric.RUNTIME_EXT_DIR, "");
    final List<String> dirs = ImmutableList.copyOf(Splitter.on(';').omitEmptyStrings().trimResults().split(extDirs));

    return CacheBuilder.newBuilder().build(new CacheLoader<ProgramType, ProgramRuntimeProvider>() {
      @Override
      public ProgramRuntimeProvider load(ProgramType programType) throws Exception {
        // Goes through all extension directory and see which service loader supports the give program type
        for (String dir : dirs) {
          File extDir = new File(dir);
          if (!extDir.isDirectory()) {
            continue;
          }

          // Each module would be under a directory of the extension directory
          for (File moduleDir : DirUtils.listFiles(extDir)) {
            if (!moduleDir.isDirectory()) {
              continue;
            }
            // Try to find a provider that can support the given program type.
            try {
              ProgramRuntimeProvider provider = findProvider(serviceLoaderCache.getUnchecked(moduleDir), programType);
              if (provider != null) {
                return provider;
              }
            } catch (Exception e) {
              LOG.warn("Exception raised when loading a ProgramRuntimeProvider from {}. Extension ignored.",
                       moduleDir, e);
            }
          }
        }

        // If there is none found in the ext dir, try to look it up from the CDAP system class ClassLoader.
        // This is for the unit-test case, which extensions are part of the test dependency, hence in the
        // unit-test ClassLoader.
        // If no provider was found, returns the NOT_SUPPORTED_PROVIDER so that we won't search again for
        // this program type.
        // Cannot use null because LoadingCache doesn't allow null value
        return Objects.firstNonNull(findProvider(SYSTEM_PROGRAM_RUNNER_PROVIDER_LOADER, programType),
                                    NOT_SUPPORTED_PROVIDER);
      }
    });
  }

  /**
   * Creates a cache for caching extension directory to {@link ServiceLoader} of {@link ProgramRuntimeProvider}.
   */
  private LoadingCache<File, ServiceLoader<ProgramRuntimeProvider>> createServiceLoaderCache() {
    return CacheBuilder.newBuilder().build(new CacheLoader<File, ServiceLoader<ProgramRuntimeProvider>>() {
      @Override
      public ServiceLoader<ProgramRuntimeProvider> load(File dir) throws Exception {
        return createServiceLoader(dir);
      }
    });
  }

  /**
   * Creates a {@link ServiceLoader} from the {@link ClassLoader} created by all jar files under the given directory.
   */
  private ServiceLoader<ProgramRuntimeProvider> createServiceLoader(File dir) {
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

    URLClassLoader classLoader = new URLClassLoader(urls, DefaultProgramRunnerFactory.class.getClassLoader());
    return ServiceLoader.load(ProgramRuntimeProvider.class, classLoader);
  }

  /**
   * Finds a {@link ProgramRuntimeProvider} from the given {@link ServiceLoader} that can support the given
   * {@link ProgramType}.
   */
  @Nullable
  private ProgramRuntimeProvider findProvider(ServiceLoader<ProgramRuntimeProvider> serviceLoader,
                                              ProgramType programType) {
    for (ProgramRuntimeProvider provider : serviceLoader) {
      Class<? extends ProgramRuntimeProvider> providerClass = provider.getClass();

      // See if the provide supports the required program type
      ProgramRuntimeProvider.SupportedProgramType supportedType =
        providerClass.getAnnotation(ProgramRuntimeProvider.SupportedProgramType.class);
      if (supportedType == null || !ImmutableSet.copyOf(supportedType.value()).contains(programType)) {
        continue;
      }

      // Found the provider.
      LOG.debug("ProgramRunnerProvider {} found for {} program.", provider, programType);
      return provider;
    }

    return null;
  }
}
