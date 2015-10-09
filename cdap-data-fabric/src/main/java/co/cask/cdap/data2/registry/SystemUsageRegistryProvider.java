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

package co.cask.cdap.data2.registry;

import co.cask.cdap.common.namespace.AbstractNamespaceClient;
import co.cask.cdap.data2.datafabric.dataset.LocalDatasetFramework;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetInstanceService;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetTypeManager;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.inject.Inject;
import com.google.inject.Provider;

/**
 * Provides a {@link UsageRegistry} implementation that uses
 * Constants.Bindings.SYSTEM_DATASET_FRAMEWORK named implementation of {@link DatasetFramework}.
 */
public class SystemUsageRegistryProvider implements Provider<UsageRegistry> {

  private final TransactionExecutorFactory txExecutorFactory;
  private final DatasetDefinitionRegistryFactory registryFactory;
  private final Provider<DatasetInstanceService> instances;
  private final Provider<DatasetTypeManager> types;
  private final Provider<AbstractNamespaceClient> namespaces;

  @Inject
  public SystemUsageRegistryProvider(
    TransactionExecutorFactory txExecutorFactory,
    DatasetDefinitionRegistryFactory registryFactory,
    Provider<DatasetInstanceService> instances,
    Provider<DatasetTypeManager> types,
    Provider<AbstractNamespaceClient> namespaces) {

    this.txExecutorFactory = txExecutorFactory;
    this.registryFactory = registryFactory;
    this.instances = instances;
    this.types = types;
    this.namespaces = namespaces;
  }

  @Override
  public UsageRegistry get() {
    return new DefaultUsageRegistry(
      txExecutorFactory, new LocalDatasetFramework(registryFactory, instances, types, namespaces));
  }
}
