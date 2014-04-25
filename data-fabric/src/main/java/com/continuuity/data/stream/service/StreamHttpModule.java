/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream.service;

import com.continuuity.api.flow.flowlet.StreamEvent;
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
import com.google.inject.name.Named;
import com.google.inject.name.Names;

import java.io.IOException;

/**
 * The guice module for providing bindings for StreamHttpService.
 */
public class StreamHttpModule extends PrivateModule {

  private final String filePrefix;

  /**
   * Constructor.
   *
   * @param filePrefix The stream file prefix for the file written by the stream writer.
   */
  public StreamHttpModule(String filePrefix) {
    this.filePrefix = filePrefix;
  }


  @Override
  protected void configure() {
    bindConstant().annotatedWith(Names.named(Constants.Stream.FILE_PREFIX_ANNOTATION)).to(filePrefix);
    bind(StreamFileWriterFactory.class).to(FileWriterFactory.class).in(Scopes.SINGLETON);

    bind(StreamHttpService.class).in(Scopes.SINGLETON);
    expose(StreamHttpService.class);
  }

  private static final class FileWriterFactory implements StreamFileWriterFactory {

    private final StreamAdmin streamAdmin;
    private final String filePrefix;

    @Inject
    FileWriterFactory(StreamAdmin streamAdmin, @Named(Constants.Stream.FILE_PREFIX_ANNOTATION) String filePrefix) {
      this.streamAdmin = streamAdmin;
      this.filePrefix = filePrefix;
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
