/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.runtime.procedure;

import co.cask.cdap.api.procedure.ProcedureResponse;
import com.google.common.base.Charsets;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.http.DefaultHttpChunk;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 */
final class HttpResponseWriter implements ProcedureResponse.Writer {

  private final Channel channel;

  HttpResponseWriter(Channel channel) {
    this.channel = channel;
  }

  @Override
  public ProcedureResponse.Writer write(ByteBuffer buffer) throws IOException {
    return write(ChannelBuffers.wrappedBuffer(buffer));
  }

  @Override
  public ProcedureResponse.Writer write(byte[] bytes) throws IOException {
    return write(ChannelBuffers.wrappedBuffer(bytes));
  }

  @Override
  public ProcedureResponse.Writer write(byte[] bytes, int off, int len) throws IOException {
    return write(ChannelBuffers.wrappedBuffer(bytes, off, len));
  }

  @Override
  public ProcedureResponse.Writer write(String content) throws IOException {
    return write(Charsets.UTF_8.encode(content));
  }

  @Override
  public void close() throws IOException {
    Channels.write(channel, DefaultHttpChunk.LAST_CHUNK).addListener(ChannelFutureListener.CLOSE);
  }

  private ProcedureResponse.Writer write(ChannelBuffer buffer) throws IOException {
    try {
      ChannelFuture result = Channels.write(channel, new DefaultHttpChunk(buffer));
      result.addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
      result.await();

      if (!result.isSuccess()) {
        if (result.isCancelled()) {
          throw new IOException("Write operation cancelled.");
        }
        throw new IOException(result.getCause());
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    return this;
  }
}
