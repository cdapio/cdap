package com.continuuity.data2.datafabric.dataset.instance;

import com.continuuity.data2.datafabric.dataset.DatasetsUtil;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.DatasetInstanceSpec;
import com.continuuity.internal.data.dataset.lib.table.OrderedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;

import java.util.Collection;
import java.util.concurrent.Callable;

/**
 * Manages dataset instances metadata
 */
// todo: there's ugly work with Datasets & Transactions (incl. exceptions inside txnl code) which will be revised as
//       part of open-sourcing Datasets effort
public class DatasetInstanceManager extends AbstractIdleService {

  private final TransactionSystemClient txClient;
  private final DatasetFramework mdsDatasetFramework;

  /** dataset types metadata store */
  private DatasetInstanceMDS mds;

  private TransactionAware txAware;

  /**
   * Ctor
   * @param mdsDatasetFramework dataset manager to be used to access the metadata store
   * @param txSystemClient tx client to be used to operate on the metadata store
   */
  public DatasetInstanceManager(DatasetFramework mdsDatasetFramework,
                                TransactionSystemClient txSystemClient) {
    this.mdsDatasetFramework = mdsDatasetFramework;
    this.txClient = txSystemClient;
  }

  @Override
  protected void startUp() throws Exception {
    // "null" for class being in system classpath, for mds it is always true
    OrderedTable table = DatasetsUtil.getOrCreateDataset(mdsDatasetFramework, "datasets.instance", "orderedTable",
                                                         DatasetInstanceProperties.EMPTY, null);

    this.txAware = (TransactionAware) table;
    this.mds = new DatasetInstanceMDS(table);
  }

  @Override
  protected void shutDown() throws Exception {
    mds.close();
  }

  /**
   * Adds dataset instance metadata
   * @param spec {@link DatasetInstanceSpec} of the dataset instance to be added
   */
  public void add(final DatasetInstanceSpec spec) {
    getTxExecutor().executeUnchecked(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        mds.write(spec);
      }
    });
  }

  /**
   * @param instanceName name of the dataset instance
   * @return dataset instance's {@link DatasetInstanceSpec}
   */
  public DatasetInstanceSpec get(final String instanceName) {
    return getTxExecutor().executeUnchecked(new Callable<DatasetInstanceSpec>() {
      @Override
      public DatasetInstanceSpec call() throws Exception {
        return mds.get(instanceName);
      }
    });
  }

  /**
   * @return collection of {@link DatasetInstanceSpec} of all dataset instances
   */
  public Collection<DatasetInstanceSpec> getAll() {
    return getTxExecutor().executeUnchecked(new Callable<Collection<DatasetInstanceSpec>>() {
      @Override
      public Collection<DatasetInstanceSpec> call() throws Exception {
        return mds.getAll();
      }
    });
  }

  /**
   * Deletes all instances
   */
  public void deleteAll() {
    // todo: do drop of data
    getTxExecutor().executeUnchecked(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        mds.deleteAll();
      }
    });
  }

  /**
   * Deletes dataset instance
   * @param instanceName name of the instance to delete
   * @return true if deletion succeeded, false otherwise
   */
  public boolean delete(final String instanceName) {
    return getTxExecutor().executeUnchecked(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return mds.delete(instanceName);
      }
    });
  }

  private TransactionExecutor getTxExecutor() {
    return new DefaultTransactionExecutor(txClient, ImmutableList.of(txAware));
  }
}
