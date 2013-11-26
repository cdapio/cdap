/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.gateway.handlers.procedure;

import com.ning.http.client.Request;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An implementation of {@link Request.EntityWriter} for sending request body from
 * a {@link ChannelBuffer}.
 */
public final class ChannelBufferEntityWriter implements Request.EntityWriter {

  private final ChannelBuffer buffer;

  public ChannelBufferEntityWriter(ChannelBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public void writeEntity(OutputStream out) throws IOException {
    buffer.readBytes(out, buffer.readableBytes());
  }
}
