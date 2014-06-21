package com.continuuity.gateway.handlers;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.api.dataset.table.OrderedTable;
import com.continuuity.app.store.ServiceStore;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.datafabric.ReactorDatasetNamespace;
import com.continuuity.data2.datafabric.dataset.DatasetsUtil;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.dataset2.DefaultDatasetDefinitionRegistry;
import com.continuuity.data2.dataset2.InMemoryDatasetFramework;
import com.continuuity.data2.dataset2.NamespacedDatasetFramework;
import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionFailureException;
import com.continuuity.data2.transaction.inmemory.MinimalTxSystemClient;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * DataSetService Store implements ServiceStore using DataSets without Transaction.
 */
public class DatasetServiceStore implements ServiceStore {

  private OrderedTable table;
  private TransactionExecutor txExecutor;

  @Inject
  public DatasetServiceStore(CConfiguration cConf, DefaultDatasetDefinitionRegistry dsRegistry,
                             @Named("serviceModule") DatasetModule datasetModule) throws Exception {
    DatasetFramework dsFramework = new NamespacedDatasetFramework(
      new InMemoryDatasetFramework(dsRegistry), new ReactorDatasetNamespace(cConf, DataSetAccessor.Namespace.SYSTEM));
    dsFramework.addModule("ordered", datasetModule);
    table = DatasetsUtil.getOrCreateDataset(dsFramework, Constants.Service.SERVICE_INFO_TABLE_NAME, "orderedTable",
                                            DatasetProperties.EMPTY, null);
    txExecutor = new DefaultTransactionExecutor(new MinimalTxSystemClient(), (TransactionAware) table);
  }

  @Override
  public Integer getServiceInstance(final String serviceName) throws TransactionFailureException {
    return txExecutor.execute(new TransactionExecutor.Function<Object, Integer>() {
      @Override
      public Integer apply(Object input) throws Exception {
        String count = Bytes.toString(table.get(Bytes.toBytes(serviceName), Bytes.toBytes("instance")));
        return (count != null) ? Integer.valueOf(count) : null;
      }
    }, null);
  }

  @Override
  public void updateServiceInstance(final String serviceName, final int instances) throws TransactionFailureException {
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        table.put(Bytes.toBytes(serviceName), Bytes.toBytes("instance"), Bytes.toBytes(String.valueOf(instances)));
      }
    });
  }
}
