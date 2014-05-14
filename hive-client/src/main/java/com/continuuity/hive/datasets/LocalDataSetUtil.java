package com.continuuity.hive.datasets;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.data.DataFabric;
import com.continuuity.data.DataFabric2Impl;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.LocalDataSetAccessor;
import com.continuuity.data.dataset.DataSetInstantiator;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableService;
import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.inmemory.InMemoryTxSystemClient;
import com.continuuity.gateway.handlers.dataset.DataSetInstantiatorFromMetaData;
import com.continuuity.hive.inmemory.LocalHiveServer;
import com.continuuity.internal.app.store.MDTBasedStoreFactory;
import com.continuuity.metadata.SerializingMetaDataTable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Utility to access data sets in local mode.
 */
public class LocalDataSetUtil {
  private CConfiguration configuration;

  private LocationFactory locationFactory;
  private TransactionSystemClient txClient;
  private DiscoveryServiceClient discoveryClient;

  public LocalDataSetUtil(CConfiguration conf) {
    this.configuration = conf;
    this.locationFactory = new LocalLocationFactory(new File(configuration.get(Constants.CFG_LOCAL_DATA_DIR)));
    this.txClient = LocalHiveServer.getTransactionSystemClient();
    this.discoveryClient = LocalHiveServer.getDiscoveryServiceClient();
  }

  public DataSetSpecification getDataSetSpecification(String accountName,
                                                      String datasetName) throws IOException {
    // application name is not needed for the context
    OperationContext opContext = new OperationContext(accountName, null);
    DataSetAccessor accessor = new LocalDataSetAccessor(configuration, LevelDBOcTableService.getInstance());

    DataSetInstantiatorFromMetaData instantiator =
      new DataSetInstantiatorFromMetaData(
        locationFactory,
        accessor,
        new MDTBasedStoreFactory(configuration,
                                 new SerializingMetaDataTable(new TransactionExecutorFactory() {
                                   @Override
                                   public TransactionExecutor createExecutor(Iterable<TransactionAware> txAwares) {
                                     return new DefaultTransactionExecutor(txClient, txAwares);
                                   }
                                 }, accessor),
                                 locationFactory));

    instantiator.init(new TimeLimitEndpointStrategy(new RandomEndpointStrategy(discoveryClient.discover(
      Constants.Service.APP_FABRIC_HTTP)), 1L, TimeUnit.SECONDS));
    return instantiator.getDataSetSpecification(datasetName, opContext);
  }

  public DataSet getDataSetInstance(DataSetSpecification spec, Transaction tx) throws IOException {
    DataSetAccessor accessor = new LocalDataSetAccessor(configuration, LevelDBOcTableService.getInstance());
    DataFabric dataFabric = new DataFabric2Impl(locationFactory, accessor);

    DataSetInstantiator instantiator = new DataSetInstantiator(dataFabric, this.getClass().getClassLoader());
    instantiator.addDataSet(spec);
    DataSet ds = instantiator.getDataSet(spec.getName());
    for (TransactionAware txAware : instantiator.getTransactionAware()) {
      txAware.startTx(tx);
    }
    return ds;
  }

}
