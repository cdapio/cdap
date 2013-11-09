package com.continuuity.metadata;

import com.continuuity.data2.OperationException;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.google.common.base.Throwables;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * HBase Serialize meta data store test.
 */
public class HBaseSerializingMetaDataStoreTest extends HBaseMetaDataStoreTest {

  @BeforeClass
  public static void setupMDS() throws Exception {
    injector.getInstance(InMemoryTransactionManager.class).startAndWait();
    mds = new SerializingMetaDataTable(injector.getInstance(TransactionExecutorFactory.class),
                                        injector.getInstance(DataSetAccessor.class));
  }

  void clearMetaData() throws OperationException {
    try {
      injector.getInstance(DataSetAccessor.class)
              .getDataSetManager(OrderedColumnarTable.class, DataSetAccessor.Namespace.SYSTEM)
              .truncate(MetaDataTable.META_DATA_TABLE_NAME);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
  // Tests that do not work on Vanilla HBase

  @Override @Test @Ignore
  public void testConcurrentSwapField() throws Exception {  }

  /**
   * Currently not working.  Will be fixed in ENG-1840.
   */
  @Override @Test @Ignore
  public void testConcurrentUpdate() throws Exception {  }
}
