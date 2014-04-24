package com.continuuity.data2.datafabric.dataset.instance;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.datafabric.ReactorDatasetNamespace;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeManager;
import com.continuuity.data2.dataset2.manager.DatasetManager;
import com.continuuity.data2.dataset2.manager.NamespacedDatasetManager;
import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.DatasetInstanceSpec;
import com.continuuity.internal.data.dataset.lib.table.OrderedTable;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.Callable;

/**
 * Manages dataset instances metadata
 */
// todo: there's ugly work with Datasets & Transactions (incl. exceptions inside txnl code) which will be revised as
//       part of open-sourcing Datasets effort
public class DatasetInstanceManager extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetTypeManager.class);

  private final TransactionSystemClient txClient;
  private final DatasetManager mdsDatasetManager;
  /** guards {@link #mds} */
  private final Object mdsGuard = new Object();

  /** dataset types metadata store */
  private DatasetInstanceMDS mds;

  private TransactionAware txAware;

  /**
   * Ctor
   * @param mdsDatasetManager dataset manager to be used to access the metadata store
   * @param txSystemClient tx client to be used to operate on the metadata store
   */
  public DatasetInstanceManager(DatasetManager mdsDatasetManager,
                                TransactionSystemClient txSystemClient,
                                CConfiguration conf) {
    this.mdsDatasetManager =
      new NamespacedDatasetManager(mdsDatasetManager,
                                   new ReactorDatasetNamespace(conf, DataSetAccessor.Namespace.SYSTEM));
    this.txClient = txSystemClient;
  }

  @Override
  protected void startUp() throws Exception {
    OrderedTable table = getMDSTable(mdsDatasetManager, "datasets.instance");
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
        synchronized (mdsGuard) {
          mds.write(spec);
        }
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
        synchronized (mdsGuard) {
          return mds.get(instanceName);
        }
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
        synchronized (mdsGuard) {
          return mds.getAll();
        }
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
        synchronized (mdsGuard) {
          return mds.delete(instanceName);
        }
      }
    });
  }

  private TransactionExecutor getTxExecutor() {
    return new DefaultTransactionExecutor(txClient, ImmutableList.of(txAware));
  }

  private OrderedTable getMDSTable(DatasetManager datasetManager, String mdsTable) {
    try {
      // "null" for class being in system classpath, for mds it is always true
      DatasetAdmin admin = datasetManager.getAdmin(mdsTable, null);
      try {
        if (admin == null) {
          datasetManager.addInstance("orderedTable", mdsTable, DatasetInstanceProperties.EMPTY);
          // "null" for class being in system classpath, for mds it is always true
          admin = datasetManager.getAdmin(mdsTable, null);
          if (admin == null) {
            throw new RuntimeException("Cannot add instance of a table " + mdsTable);
          }
        }

        if (!admin.exists()) {
          admin.create();
        }

        // "null" for class being in system classpath, for mds it is always true
        return (OrderedTable) datasetManager.getDataset(mdsTable, null);
      } finally {
        if (admin != null) {
          admin.close();
        }
      }
    } catch (Exception e) {
      LOG.error("Could not get access to MDS table", e);
      throw Throwables.propagate(e);
    }
  }
}
