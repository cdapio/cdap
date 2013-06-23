package com.continuuity.data.metadata;

import org.junit.BeforeClass;

public class LevelDBSerializingMetaDataStoreTest extends
    LevelDBMetaDataStoreTest {

  @BeforeClass
  public static void setupMDS() throws Exception {
    mds = new SerializingMetaDataStore(opex);
  }

}
