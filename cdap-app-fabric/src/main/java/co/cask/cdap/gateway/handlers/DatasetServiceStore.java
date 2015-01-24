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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.app.store.ServiceStore;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.cdap.data2.dataset2.lib.kv.NoTxKeyValueTable;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionFailureException;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * DatasetService Store implements ServiceStore using Datasets without Transaction.
 */
public final class DatasetServiceStore extends AbstractIdleService implements ServiceStore {
  private final DatasetFramework dsFramework;
  private NoTxKeyValueTable table;

  @Inject
  public DatasetServiceStore(CConfiguration cConf, DatasetDefinitionRegistryFactory dsRegistryFactory,
                             @Named("serviceModule") DatasetModule datasetModule) throws Exception {
    this.dsFramework = new NamespacedDatasetFramework(new InMemoryDatasetFramework(dsRegistryFactory, cConf),
                                                      new DefaultDatasetNamespace(cConf));
  }

  @Override
  public synchronized Integer getServiceInstance(final String serviceName) throws TransactionFailureException {
    String count = Bytes.toString(table.get(Bytes.toBytes(serviceName)));
    return (count != null) ? Integer.valueOf(count) : null;
  }

  @Override
  public synchronized void setServiceInstance(final String serviceName, final int instances) {
    table.put(Bytes.toBytes(serviceName), Bytes.toBytes(String.valueOf(instances)));
  }

  @Override
  protected void startUp() throws Exception {
    Id.DatasetInstance serviceStoreDatasetInstanceId =
      Id.DatasetInstance.from(Constants.SYSTEM_NAMESPACE, Constants.Service.SERVICE_INSTANCE_TABLE_NAME);
    table = DatasetsUtil.getOrCreateDataset(dsFramework, serviceStoreDatasetInstanceId,
                                            NoTxKeyValueTable.class.getName(),
                                            DatasetProperties.EMPTY, null, null);
  }

  @Override
  protected void shutDown() throws Exception {
    table.close();
  }
}
