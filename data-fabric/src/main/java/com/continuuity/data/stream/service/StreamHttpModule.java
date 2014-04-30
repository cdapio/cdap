/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream.service;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.file.FileWriter;
import com.continuuity.data.stream.StreamFileWriterFactory;
import com.continuuity.data.stream.TimePartitionedStreamFileWriter;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.data2.transaction.stream.StreamConfig;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;

import java.io.IOException;

/**
 * The guice module for providing bindings for StreamHttpService.
 */
public class StreamHttpModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(StreamFileWriterFactory.class).to(FileWriterFactory.class).in(Scopes.SINGLETON);

    bind(StreamHttpService.class).in(Scopes.SINGLETON);
    expose(StreamHttpService.class);
  }

  private static final class FileWriterFactory implements StreamFileWriterFactory {

    private final StreamAdmin streamAdmin;
    private final String filePrefix;

    @Inject
    FileWriterFactory(CConfiguration cConf, StreamAdmin streamAdmin) {
      this.streamAdmin = streamAdmin;
      this.filePrefix = cConf.get(Constants.Stream.FILE_PREFIX);
    }

    @Override
    public FileWriter<StreamEvent> create(String streamName) throws IOException {
      try {
        StreamConfig config = streamAdmin.getConfig(streamName);
        return new TimePartitionedStreamFileWriter(config, filePrefix);

      } catch (Exception e) {
        Throwables.propagateIfPossible(e, IOException.class);
        throw new IOException(e);
      }
    }
  }
}
