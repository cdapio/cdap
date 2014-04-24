package com.continuuity.data2.dataset2.manager;

import com.continuuity.internal.data.dataset.Dataset;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.module.DatasetModule;

import java.io.IOException;

/**
 * Wrapper for {@link DatasetManager} that namespaces dataset instances names.
 */
public class NamespacedDatasetManager implements DatasetManager {
  private final DatasetNamespace namespace;
  private final DatasetManager delegate;

  public NamespacedDatasetManager(DatasetManager delegate, DatasetNamespace namespace) {
    this.delegate = delegate;
    this.namespace = namespace;
  }

  @Override
  public synchronized void register(String moduleName, Class<? extends DatasetModule> moduleClass)
    throws ModuleConflictException, IOException {

    delegate.register(moduleName, moduleClass);
  }

  @Override
  public void deleteModule(String moduleName) throws ModuleConflictException, IOException {
    delegate.deleteModule(moduleName);
  }

  @Override
  public synchronized void addInstance(String datasetType, String datasetInstanceName, DatasetInstanceProperties props)
    throws InstanceConflictException, IOException {

    delegate.addInstance(datasetType, namespace(datasetInstanceName), props);
  }

  @Override
  public void deleteInstance(String datasetInstanceName) throws InstanceConflictException, IOException {
    delegate.deleteInstance(namespace(datasetInstanceName));
  }

  @Override
  public synchronized <T extends DatasetAdmin> T getAdmin(String datasetInstanceName, ClassLoader classLoader)
    throws IOException {

    return delegate.getAdmin(namespace(datasetInstanceName), classLoader);
  }

  @Override
  public synchronized <T extends Dataset> T getDataset(String datasetInstanceName, ClassLoader ignored)
    throws IOException {

    return delegate.getDataset(namespace(datasetInstanceName), ignored);
  }

  private String namespace(String datasetInstanceName) {
    return namespace.namespace(datasetInstanceName);
  }
}
