/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.io.Locations;
import com.continuuity.common.io.SeekableInputStream;
import com.continuuity.data.file.FileReader;
import com.google.common.base.Function;
import com.google.common.io.InputSupplier;
import org.apache.twill.filesystem.Location;

import javax.annotation.Nullable;

/**
 *
 */
public final class StreamFileReaderFactory implements Function<StreamFileOffset, FileReader<StreamEvent, Long>> {

  @Override
  public FileReader<StreamEvent, Long> apply(StreamFileOffset input) {
    return StreamDataFileReader.createWithOffset(createInputSupplier(input.getEventLocation()),
                                                 createInputSupplier(input.getIndexLocation()),
                                                 input.getOffset());
  }

  private InputSupplier<? extends SeekableInputStream> createInputSupplier(@Nullable Location location) {
    return location == null ? null : Locations.newInputSupplier(location);
  }
}
