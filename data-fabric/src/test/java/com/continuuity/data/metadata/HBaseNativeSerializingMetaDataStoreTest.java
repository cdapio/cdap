package com.continuuity.data.metadata;

import org.junit.Ignore;
import org.junit.Test;

public class HBaseNativeSerializingMetaDataStoreTest extends HBaseNativeMetaDataStoreTest {

  // Tests that do not work on patched HBase (called HBaseNative)

  @Override @Test @Ignore
  public void testConcurrentSwapField() throws Exception {  }
}