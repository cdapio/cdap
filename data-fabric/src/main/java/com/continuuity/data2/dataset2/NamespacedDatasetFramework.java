package com.continuuity.data2.dataset2;

import com.continuuity.internal.data.dataset.Dataset;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.module.DatasetModule;

import java.io.IOException;

/**
 * Wrapper for {@link DatasetFramework} that namespaces dataset instances names.
 */
public class NamespacedDatasetFramework implements DatasetFramework {
  private final DatasetNamespace namespace;
  private final DatasetFramework delegate;

  public NamespacedDatasetFramework(DatasetFramework delegate, DatasetNamespace namespace) {
    this.delegate = delegate;
    this.namespace = namespace;
  }

  @Override
  public void register(String moduleName, Class<? extends DatasetModule> moduleClass)
    throws DatasetManagementException {

    delegate.register(moduleName, moduleClass);
  }

  @Override
  public void deleteModule(String moduleName) throws DatasetManagementException {
    delegate.deleteModule(moduleName);
  }

  @Override
  public void addInstance(String datasetType, String datasetInstanceName, DatasetInstanceProperties props)
    throws DatasetManagementException {

    delegate.addInstance(datasetType, namespace(datasetInstanceName), props);
  }

  @Override
  public void deleteInstance(String datasetInstanceName) throws DatasetManagementException {
    delegate.deleteInstance(namespace(datasetInstanceName));
  }

  @Override
  public <T extends DatasetAdmin> T getAdmin(String datasetInstanceName, ClassLoader classLoader)
    throws DatasetManagementException, IOException {

    return delegate.getAdmin(namespace(datasetInstanceName), classLoader);
  }

  @Override
  public <T extends Dataset> T getDataset(String datasetInstanceName, ClassLoader ignored)
    throws DatasetManagementException, IOException {

    return delegate.getDataset(namespace(datasetInstanceName), ignored);
  }

  private String namespace(String datasetInstanceName) {
    return namespace.namespace(datasetInstanceName);
  }
}
