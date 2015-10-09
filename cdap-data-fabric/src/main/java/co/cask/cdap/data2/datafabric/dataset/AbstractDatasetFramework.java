/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset;

import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetClassLoaderProvider;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.module.lib.DatasetModules;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.Id;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.ApplicationBundler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;

/**
 * Base {@link DatasetFramework} implementation.
 */
@SuppressWarnings("unchecked")
public abstract class AbstractDatasetFramework implements DatasetFramework {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractDatasetFramework.class);
  private final DatasetDefinitionRegistryFactory registryFactory;

  AbstractDatasetFramework(DatasetDefinitionRegistryFactory registryFactory) {
    this.registryFactory = registryFactory;
  }

  protected abstract void addModule(Id.DatasetModule module, String typeName,
                                    Location jar) throws DatasetManagementException;

  // can be used directly if DatasetTypeMeta is known, like in create dataset by dataset ops executor service
  @Override
  public <T extends DatasetType> T getDatasetType(DatasetTypeMeta implementationInfo,
                                                  ClassLoader classLoader,
                                                  DatasetClassLoaderProvider classLoaderProvider) {

    if (classLoader == null) {
      classLoader = Objects.firstNonNull(Thread.currentThread().getContextClassLoader(), getClass().getClassLoader());
    }

    DatasetDefinitionRegistry registry = registryFactory.create();
    List<DatasetModuleMeta> modulesToLoad = implementationInfo.getModules();
    for (DatasetModuleMeta moduleMeta : modulesToLoad) {
      // adding dataset module jar to classloader
      try {
        classLoader = classLoaderProvider.get(moduleMeta, classLoader);
      } catch (IOException e) {
        LOG.error("Was not able to init classloader for module {} while trying to load type {}",
                  moduleMeta, implementationInfo, e);
        throw Throwables.propagate(e);
      }

      Class<?> moduleClass;

      // try program class loader then cdap class loader
      try {
        moduleClass = ClassLoaders.loadClass(moduleMeta.getClassName(), classLoader, this);
      } catch (ClassNotFoundException e) {
        try {
          moduleClass = ClassLoaders.loadClass(moduleMeta.getClassName(), null, this);
        } catch (ClassNotFoundException e2) {
          LOG.error("Was not able to load dataset module class {} while trying to load type {}",
                    moduleMeta.getClassName(), implementationInfo, e);
          throw Throwables.propagate(e);
        }
      }

      try {
        DatasetModule module = DatasetModules.getDatasetModule(moduleClass);
        module.register(registry);
      } catch (Exception e) {
        LOG.error("Was not able to load dataset module class {} while trying to load type {}",
                  moduleMeta.getClassName(), implementationInfo, e);
        throw Throwables.propagate(e);
      }
    }

    // contract of DatasetTypeMeta is that the last module returned by getModules() is the one
    // that announces the dataset's type. The classloader for the returned DatasetType must be the classloader
    // for that last module.
    return (T) new DatasetType(registry.get(implementationInfo.getName()), classLoader);
  }

  protected void addModule(Id.DatasetModule moduleId, Class<?> typeClass,
                           boolean deleteJarFile) throws DatasetManagementException {
    try {
      File tempFile = File.createTempFile(typeClass.getName(), ".jar");
      try {
        Location tempJarPath = createDeploymentJar(typeClass, new LocalLocationFactory().create(tempFile.toURI()));
        addModule(moduleId, typeClass.getName(), tempJarPath);
      } finally {
        if (deleteJarFile) {
          if (!tempFile.delete()) {
            LOG.warn("Failed to delete temp jar file: {}", tempFile.getAbsolutePath());
          }
        }
      }
    } catch (DatasetManagementException e) {
      throw e;
    } catch (Exception e) {
      String msg = String.format("Could not create jar for deploying dataset module %s with main class %s",
                                 moduleId, typeClass.getName());
      throw new DatasetManagementException(msg, e);
    }
  }

  protected static Location createDeploymentJar(Class<?> clz, Location destination) throws IOException {
    Location tempBundle = destination.getTempFile(".jar");
    try {
      ClassLoader remembered = Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader(clz.getClassLoader());
      try {
        ApplicationBundler bundler = new ApplicationBundler(ImmutableList.of("co.cask.cdap.api",
                                                                             "org.apache.hadoop",
                                                                             "org.apache.hbase",
                                                                             "org.apache.hive"));
        bundler.createBundle(tempBundle, clz);
      } finally {
        Thread.currentThread().setContextClassLoader(remembered);
      }

      // Create the program jar for deployment. It removes the "classes/" prefix as that's the convention taken
      // by the ApplicationBundler inside Twill.
      try (
        JarOutputStream jarOutput = new JarOutputStream(destination.getOutputStream());
        JarInputStream jarInput = new JarInputStream(tempBundle.getInputStream())
      ) {
        Set<String> seen = Sets.newHashSet();
        JarEntry jarEntry = jarInput.getNextJarEntry();
        while (jarEntry != null) {
          boolean isDir = jarEntry.isDirectory();
          String entryName = jarEntry.getName();
          if (!entryName.equals("classes/")) {
            if (entryName.startsWith("classes/")) {
              jarEntry = new JarEntry(entryName.substring("classes/".length()));
            } else {
              jarEntry = new JarEntry(entryName);
            }
            if (seen.add(jarEntry.getName())) {
              jarOutput.putNextEntry(jarEntry);

              if (!isDir) {
                ByteStreams.copy(jarInput, jarOutput);
              }
            }
          }

          jarEntry = jarInput.getNextJarEntry();
        }
      }

      return destination;
    } finally {
      tempBundle.delete();
    }
  }

}
