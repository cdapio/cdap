package com.continuuity.data.metadata;

import org.junit.BeforeClass;

/**
 * Hypersql backed metadata store tests.
 */
public class HyperSQLSerializingMetaDataStoreTest extends
    HyperSQLMetaDataStoreTest {

  @BeforeClass
  public static void setupMDS() throws Exception {
    mds = new SerializingMetaDataStore(opex);
  }

}
