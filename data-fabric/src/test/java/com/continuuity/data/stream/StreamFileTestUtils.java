/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.api.stream.StreamEventData;
import com.continuuity.common.io.SeekableInputStream;
import com.continuuity.common.stream.DefaultStreamEventData;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.InputSupplier;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Helper methods for writing Stream file tests.
 */
public class StreamFileTestUtils {

  private StreamFileTestUtils() {
  }

  public static InputSupplier<? extends SeekableInputStream> createInputSupplier(final File file) {
    return new InputSupplier<SeekableInputStream>() {
      @Override
      public SeekableInputStream getInput() throws IOException {
        return SeekableInputStream.create(new FileInputStream(file));
      }
    };
  }

  public static StreamEventData createData(String body) {
    return new DefaultStreamEventData(ImmutableMap.<String, String>of(), Charsets.UTF_8.encode(body));
  }
}
