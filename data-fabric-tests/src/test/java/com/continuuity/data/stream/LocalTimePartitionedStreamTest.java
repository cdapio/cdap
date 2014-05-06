/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 *
 */
public class LocalTimePartitionedStreamTest extends TimePartitionedStreamTestBase {

  private static LocationFactory locationFactory;

  @BeforeClass
  public static void init() throws IOException {
    locationFactory = new LocalLocationFactory(tmpFolder.newFolder());
  }

  @Override
  protected LocationFactory getLocationFactory() {
    return locationFactory;
  }
}
