/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.DatasetModule;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;

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
  public void addModule(String moduleName, DatasetModule module)
    throws DatasetManagementException {

    delegate.addModule(moduleName, module);
  }

  @Override
  public void deleteModule(String moduleName) throws DatasetManagementException {
    delegate.deleteModule(moduleName);
  }

  @Override
  public void deleteAllModules() throws DatasetManagementException {
    delegate.deleteAllModules();
  }

  @Override
  public void addInstance(String datasetType, String datasetInstanceName, DatasetProperties props)
    throws DatasetManagementException, IOException {

    delegate.addInstance(datasetType, namespace(datasetInstanceName), props);
  }

  @Override
  public void updateInstance(String datasetInstanceName, DatasetProperties props)
    throws DatasetManagementException, IOException {
    delegate.updateInstance(namespace(datasetInstanceName), props);
  }

  @Override
  public Collection<DatasetSpecification> getInstances() throws DatasetManagementException {
    Collection<DatasetSpecification> specs = delegate.getInstances();
    // client may pass the name back e.g. do delete instance, so we need to un-namespace it
    ImmutableList.Builder<DatasetSpecification> builder = ImmutableList.builder();
    for (DatasetSpecification spec : specs) {
      DatasetSpecification s = fromNamespaced(spec);
      if (s != null) {
        builder.add(s);
      }
    }
    return builder.build();
  }

  @Nullable
  @Override
  public DatasetSpecification getDatasetSpec(String name) throws DatasetManagementException {
    return fromNamespaced(delegate.getDatasetSpec(namespace(name)));
  }

  @Override
  public boolean hasInstance(String instanceName) throws DatasetManagementException {
    return delegate.hasInstance(namespace(instanceName));
  }

  @Override
  public boolean hasType(String typeName) throws DatasetManagementException {
    return delegate.hasType(typeName);
  }

  @Override
  public void deleteInstance(String datasetInstanceName) throws DatasetManagementException, IOException {
    delegate.deleteInstance(namespace(datasetInstanceName));
  }

  @Override
  public void deleteAllInstances() throws DatasetManagementException, IOException {
    // delete all instances ONLY in this namespace
    for (DatasetSpecification spec : getInstances()) {
      deleteInstance(spec.getName());
    }
  }

  @Override
  public <T extends DatasetAdmin> T getAdmin(String datasetInstanceName, ClassLoader classLoader)
    throws DatasetManagementException, IOException {

    return delegate.getAdmin(namespace(datasetInstanceName), classLoader);
  }

  @Override
  public <T extends Dataset> T getDataset(String datasetInstanceName, Map<String, String> arguments,
                                          ClassLoader classLoader)
    throws DatasetManagementException, IOException {

    return delegate.getDataset(namespace(datasetInstanceName), arguments, classLoader);
  }

  @Nullable
  private DatasetSpecification fromNamespaced(@Nullable DatasetSpecification spec) {
    if (spec == null) {
      return null;
    }
    String notNamespaced = namespace.fromNamespaced(spec.getName());
    return notNamespaced == null ? null : DatasetSpecification.changeName(spec, notNamespaced);
  }

  private String namespace(String datasetInstanceName) {
    return namespace.namespace(datasetInstanceName);
  }
}
