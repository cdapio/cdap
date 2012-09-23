package com.continuuity.data.metadata;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class MemoryColumnarMetaDataStoreTest extends MemoryMetaDataStoreTest {

  @BeforeClass
  public static void setupMDS() throws Exception {
    mds = new ColumnarMetaDataStore(opex);
  }

  // TODO remove this once list is implemented
  @Test @Ignore @Override
  public void testList() { }

  // TODO remove this once clear is implemented
  @Test @Ignore @Override
  public void testClear() { }

}
