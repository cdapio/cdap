/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.common.logging;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferIndexFinder;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.slf4j.Logger;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;

/**
 * Redirected {@link PrintStream} to logger
 */
public final class RedirectedPrintStream extends FilterOutputStream {

  private final ChannelBuffer buffer;
  private final PrintStream outStream;
  private final Logger logger;
  private final boolean isErrorStream;

  /**
   * Creates {@link RedirectedPrintStream} from given outstream which will be redirected to given logger.
   * @param logger logger object to which outstream will be redirected to.
   * @param outStream output stream which will be redirected to logger object.
   * @return The instance of a {@link RedirectedPrintStream}.
   */
  public static RedirectedPrintStream createRedirectedOutStream(Logger logger, @Nullable PrintStream outStream) {
    return new RedirectedPrintStream(logger, outStream, false);
  }

  /**
   * Creates {@link RedirectedPrintStream} from given errorstream which will be redirected to given logger.
   * @param logger logger object to which outstream will be redirected to.
   * @param errorStream error stream which will be redirected to logger object.
   * @return The instance of a {@link RedirectedPrintStream}.
   */
  public static RedirectedPrintStream createRedirectedErrStream(Logger logger, @Nullable PrintStream errorStream) {
    return new RedirectedPrintStream(logger, errorStream, true);
  }

  private RedirectedPrintStream(Logger logger, @Nullable PrintStream outStream, boolean isErrorStream) {
    super(new ChannelBufferOutputStream(ChannelBuffers.dynamicBuffer()));
    // Safe cast as we know what outputStream we've created.
    this.buffer = ((ChannelBufferOutputStream) out).buffer();
    this.logger = logger;
    this.outStream = outStream;
    this.isErrorStream = isErrorStream;
  }

  @Override
  public void flush() throws IOException {
    if (outStream != null) {
      outStream.flush();
    }

    out.flush();

    // Write out buffered data, line by line.
    // The last line may not be written out if it doesn't have a line separator.
    int len = buffer.bytesBefore(ChannelBufferIndexFinder.CRLF);
    while (len > 0) {
      log(buffer.readSlice(len).toString(StandardCharsets.UTF_8));

      // Skip the CRLF.
      while (buffer.bytesBefore(ChannelBufferIndexFinder.CRLF) == 0) {
        buffer.readByte();
      }
      len = buffer.bytesBefore(ChannelBufferIndexFinder.CRLF);
    }

    if (!buffer.readable()) {
      buffer.clear();
    } else {
      buffer.discardReadBytes();
    }
  }

  @Override
  public void close() throws IOException {
    flush();
    // Log whatever remaining. There shouldn't be line separator anymore after the flush() call.
    log(buffer.toString(StandardCharsets.UTF_8));
  }

  @Override
  public void write(int i) throws IOException {
    out.write(i);
    if (outStream != null) {
      outStream.write(i);
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    out.write(b, off, len);
    if (outStream != null) {
      outStream.write(b, off, len);
    }
  }

  private void log(String line) {
    if (line.isEmpty()) {
      return;
    }
    if (isErrorStream) {
      logger.warn(line);
    } else {
      logger.info(line);
    }
  }
}
