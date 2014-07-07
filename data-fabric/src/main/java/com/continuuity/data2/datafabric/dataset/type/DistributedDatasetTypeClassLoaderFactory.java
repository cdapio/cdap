package com.continuuity.data2.datafabric.dataset.type;

import com.continuuity.common.lang.ClassLoaders;
import com.continuuity.common.lang.jar.BundleJarUtil;
import com.google.common.io.Files;
import com.google.inject.Inject;
import org.apache.twill.filesystem.LocationFactory;

import java.io.File;
import java.io.IOException;

/**
 * Creates a {@link ClassLoader} for a {@link DatasetModuleMeta} by unpacking the moduleMeta jar,
 * since the moduleMeta jar may not be present in the local filesystem.
 */
public class DistributedDatasetTypeClassLoaderFactory implements DatasetTypeClassLoaderFactory {

  private final LocationFactory locationFactory;

  @Inject
  public DistributedDatasetTypeClassLoaderFactory(LocationFactory locationFactory) {
    this.locationFactory = locationFactory;
  }

  @Override
  public ClassLoader create(DatasetModuleMeta moduleMeta, ClassLoader parentClassLoader) throws IOException {
    if (moduleMeta.getJarLocation() == null) {
      return parentClassLoader;
    }

    // creating tempDir is fine since it will be created inside a YARN container, so it will be cleaned up
    File tempDir = Files.createTempDir();
    BundleJarUtil.unpackProgramJar(locationFactory.create(moduleMeta.getJarLocation()), tempDir);
    return ClassLoaders.newProgramClassLoaderWithoutFilter(tempDir, parentClassLoader);
  }
}
