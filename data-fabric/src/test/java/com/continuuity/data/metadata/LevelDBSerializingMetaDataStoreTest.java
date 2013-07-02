package com.continuuity.data.metadata;

import org.junit.BeforeClass;

/**
 * LevelDB backed metadata store tests.
 */
public class LevelDBSerializingMetaDataStoreTest extends
    LevelDBMetaDataStoreTest {

  @BeforeClass
  public static void setupMDS() throws Exception {
    mds = new SerializingMetaDataStore(opex);
  }

}
