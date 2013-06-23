package com.continuuity.data.metadata;

import org.junit.BeforeClass;

public class HyperSQLSerializingMetaDataStoreTest extends
    HyperSQLMetaDataStoreTest {

  @BeforeClass
  public static void setupMDS() throws Exception {
    mds = new SerializingMetaDataStore(opex);
  }

}
