package com.continuuity.data.metadata;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class MemorySerializingMetaDataStoreTest extends MemoryMetaDataStoreTest {

  @BeforeClass
  public static void setupMDS() throws Exception {
    mds = new SerializingMetaDataStore(opex);
  }

  /**
   * Currently not working.  Will be fixed in ENG-2161.
   */
  @Test @Override @Ignore
  public void testConcurrentSwapField() throws Exception {}
}
