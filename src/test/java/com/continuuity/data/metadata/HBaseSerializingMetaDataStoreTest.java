package com.continuuity.data.metadata;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class HBaseSerializingMetaDataStoreTest extends
    HBaseMetaDataStoreTest {

  @BeforeClass
  public static void setupMDS() throws Exception {
    mds = new SerializingMetaDataStore(opex);
  }

  @Override @Test @Ignore
  public void testSwapField() throws Exception {  }
}