/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.stream.DefaultStreamEvent;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;

/**
 * Helper methods for writing Stream file tests.
 */
public class StreamFileTestUtils {

  private StreamFileTestUtils() {
  }

  public static StreamEvent createEvent(long timestamp, String body) {
    return new DefaultStreamEvent(ImmutableMap.<String, String>of(), Charsets.UTF_8.encode(body), timestamp);
  }

  public static Location createTempDir(LocationFactory locationFactory) {
    try {
      Location dir = locationFactory.create("/").getTempFile(".dir");
      dir.mkdirs();
      return dir;
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
