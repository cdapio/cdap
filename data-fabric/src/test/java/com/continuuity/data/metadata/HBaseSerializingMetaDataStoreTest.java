package com.continuuity.data.metadata;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * HBase Serialize meta data store test.
 */
public class HBaseSerializingMetaDataStoreTest extends HBaseMetaDataStoreTest {

  private static OperationExecutor opex;

  @BeforeClass
  public static void setupMDS() throws Exception {
    opex = injector.getInstance(Key.get(OperationExecutor.class, Names.named("DataFabricOperationExecutor")));
    mds = new SerializingMetaDataStore(opex);
  }

  void clearMetaData() throws OperationException {
    opex.execute(context, new ClearFabric(ClearFabric.ToClear.META));
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
