package com.continuuity.data.metadata;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class HBaseColumnarMetaDataStoreTest extends
    HBaseMetaDataStoreTest {

  @BeforeClass
  public static void setupMDS() throws Exception {
    mds = new ColumnarMetaDataStore(opex);
  }

  @Test @Ignore @Override
  public void testList() { }

}
