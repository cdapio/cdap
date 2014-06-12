/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.runtime;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.io.Locations;
import com.continuuity.data.file.FileWriter;
import com.continuuity.data.stream.StreamFileWriterFactory;
import com.continuuity.data.stream.StreamUtils;
import com.continuuity.data.stream.TimePartitionedStreamFileWriter;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.data2.transaction.stream.StreamConfig;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;

import java.io.IOException;

/**
 * A {@link StreamFileWriterFactory} that provides {@link FileWriter} which writes to file location.
 * Use for both local and distributed mode.
 */
public final class LocationStreamFileWriterFactory implements StreamFileWriterFactory {

  private final StreamAdmin streamAdmin;
  private final String filePrefix;

  @Inject
  LocationStreamFileWriterFactory(CConfiguration cConf, StreamAdmin streamAdmin) {
    this.streamAdmin = streamAdmin;
    this.filePrefix = String.format("%s.%d",
                                    cConf.get(Constants.Stream.FILE_PREFIX),
                                    cConf.getInt(Constants.Stream.CONTAINER_INSTANCE_ID, 0));
  }

  @Override
  public FileWriter<StreamEvent> create(StreamConfig config, int generation) throws IOException {
    try {
      Preconditions.checkNotNull(config.getLocation(), "Location for stream {} is unknown.", config.getName());

      Location baseLocation = StreamUtils.createGenerationLocation(config.getLocation(), generation);
      Locations.mkdirsIfNotExists(baseLocation);

      return new TimePartitionedStreamFileWriter(baseLocation, config.getPartitionDuration(),
                                                 filePrefix, config.getIndexInterval());

    } catch (Exception e) {
      Throwables.propagateIfPossible(e, IOException.class);
      throw new IOException(e);
    }
  }
}
