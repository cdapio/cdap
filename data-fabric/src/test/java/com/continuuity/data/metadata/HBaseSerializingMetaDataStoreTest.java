package com.continuuity.data.metadata;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * HBase Serialize meta data store test.
 */
public class HBaseSerializingMetaDataStoreTest extends HBaseMetaDataStoreTest {

  @BeforeClass
  public static void setupMDS() throws Exception {
    mds = new SerializingMetaDataStore(opex);
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
