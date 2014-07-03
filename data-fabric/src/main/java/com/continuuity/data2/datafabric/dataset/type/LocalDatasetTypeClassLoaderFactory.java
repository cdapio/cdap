package com.continuuity.data2.datafabric.dataset.type;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Creates a {@link ClassLoader} for a {@link DatasetModuleMeta} without unpacking the moduleMeta jar,
 * since the jar file is already present in the local filesystem.
 */
public class LocalDatasetTypeClassLoaderFactory implements DatasetTypeClassLoaderFactory {

  @Override
  public ClassLoader create(DatasetModuleMeta moduleMeta, ClassLoader parentClassLoader) throws IOException {
    if (moduleMeta.getJarLocation() == null) {
      return parentClassLoader;
    }

    return new URLClassLoader(new URL[]{moduleMeta.getJarLocation().toURL()}, parentClassLoader);
  }
}
