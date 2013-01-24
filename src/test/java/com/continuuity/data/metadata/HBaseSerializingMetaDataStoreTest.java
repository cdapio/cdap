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

  // Tests that do not work on HBase

  @Override @Test @Ignore
  public void testConcurrentSwapField() throws Exception {  }

  /**
   * Currently not working.  Will be fixed in ???-???.
   */
  @Override @Test @Ignore
  public void testConcurrentUpdate() throws Exception {  }
}