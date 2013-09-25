package com.continuuity.data.metadata;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.google.common.base.Throwables;
import org.junit.BeforeClass;

/**
 * LevelDB backed metadata store tests.
 */
public class LevelDBSerializingMetaDataStoreTest extends LevelDBMetaDataStoreTest {

  @BeforeClass
  public static void setupMDS() throws Exception {
    injector.getInstance(InMemoryTransactionManager.class).init();
    mds = new SerializingMetaDataStore(injector.getInstance(TransactionExecutorFactory.class),
                                        injector.getInstance(DataSetAccessor.class));
  }

  void clearMetaData() throws OperationException {
    try {
      injector.getInstance(DataSetAccessor.class)
              .getDataSetManager(OrderedColumnarTable.class, DataSetAccessor.Namespace.SYSTEM)
              .truncate(MetaDataStore.META_DATA_TABLE_NAME);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

}
