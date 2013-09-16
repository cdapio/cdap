package com.continuuity.data.metadata;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.executor.OperationExecutor;
import org.junit.BeforeClass;

/**
 * LevelDB backed metadata store tests.
 */
public class LevelDBSerializingMetaDataStoreTest extends LevelDBMetaDataStoreTest {

  @BeforeClass
  public static void setupMDS() throws Exception {
    mds = new SerializingMetaDataStore(injector.getInstance(OperationExecutor.class));
  }

  void clearMetaData() throws OperationException {
    injector.getInstance(OperationExecutor.class).execute(context, new ClearFabric(ClearFabric.ToClear.META));
  }

}
