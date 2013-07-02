package com.continuuity.data.metadata;

import org.junit.BeforeClass;

/**
 * Memory metadata store tests.
 */
public class MemorySerializingMetaDataStoreTest extends MemoryMetaDataStoreTest {

  @BeforeClass
  public static void setupMDS() throws Exception {
    mds = new SerializingMetaDataStore(opex);
  }

}
