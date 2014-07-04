package com.continuuity.data2.datafabric.dataset.type;

import java.io.IOException;

/**
 * Creates a {@link ClassLoader} for a {@link DatasetModuleMeta}.
 */
public interface DatasetTypeClassLoaderFactory {

  ClassLoader create(DatasetModuleMeta moduleMeta, ClassLoader parentClassLoader) throws IOException;

}
