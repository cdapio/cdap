package com.continuuity.data2.datafabric.dataset.instance;

import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.table.OrderedTable;
import com.continuuity.data2.datafabric.dataset.DatasetMetaTableUtil;
import com.continuuity.data2.datafabric.dataset.DatasetsUtil;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionSystemClient;
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
  private final DatasetMetaTableUtil mdsTableUtil;

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
    this.mdsTableUtil = new DatasetMetaTableUtil(mdsDatasetFramework);
    this.txClient = txSystemClient;
  }

  @Override
  protected void startUp() throws Exception {
    OrderedTable table = mdsTableUtil.getInstanceMetaTable();

    this.txAware = (TransactionAware) table;
    this.mds = new DatasetInstanceMDS(table);
  }

  @Override
  protected void shutDown() throws Exception {
    mds.close();
  }

  /**
   * Adds dataset instance metadata
   * @param spec {@link com.continuuity.api.dataset.DatasetSpecification} of the dataset instance to be added
   */
  public void add(final DatasetSpecification spec) {
    getTxExecutor().executeUnchecked(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        mds.write(spec);
      }
    });
  }

  /**
   * @param instanceName name of the dataset instance
   * @return dataset instance's {@link com.continuuity.api.dataset.DatasetSpecification}
   */
  public DatasetSpecification get(final String instanceName) {
    return getTxExecutor().executeUnchecked(new Callable<DatasetSpecification>() {
      @Override
      public DatasetSpecification call() throws Exception {
        return mds.get(instanceName);
      }
    });
  }

  /**
   * @return collection of {@link com.continuuity.api.dataset.DatasetSpecification} of all dataset instances
   */
  public Collection<DatasetSpecification> getAll() {
    return getTxExecutor().executeUnchecked(new Callable<Collection<DatasetSpecification>>() {
      @Override
      public Collection<DatasetSpecification> call() throws Exception {
        return mds.getAll();
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
