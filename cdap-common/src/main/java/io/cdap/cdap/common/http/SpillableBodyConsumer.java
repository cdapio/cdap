/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.common.http;

import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import io.cdap.http.BodyConsumer;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * An abstract base class for implementing a {@link BodyConsumer} that spills data to disk when the request size
 * exceeded a predetermined in memory buffer size.
 */
public abstract class SpillableBodyConsumer extends BodyConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(SpillableBodyConsumer.class);

  private final CompositeByteBuf buffer;
  private final Path spillPath;
  private final int bufferLimit;
  private OutputStream outputStream;

  protected SpillableBodyConsumer(Path spillPath, int bufferLimit) {
    this.buffer = Unpooled.compositeBuffer();
    this.spillPath = spillPath;
    this.bufferLimit = bufferLimit;
  }

  /**
   * This method will be called to process the complete request body.
   *
   * @param inputStream the {@link InputStream} to read the request body
   * @param responder the {@link HttpResponder} for responding to client
   * @throws IOException if failed to process the given input
   */
  protected abstract void processInput(InputStream inputStream, HttpResponder responder) throws Exception;

  @Override
  public void chunk(ByteBuf request, HttpResponder responder) {
    if (outputStream == null) {
      int remaining = bufferLimit - buffer.readableBytes();
      if (remaining >= request.readableBytes()) {
        buffer.addComponent(true, request.retain());
        return;
      }
      // Otherwise, add whatever can fit in the buffer
      if (remaining > 0) {
        buffer.addComponent(true, request.readRetainedSlice(remaining));
      }
    }

    // Spill whatever remaining in the request buffer to disk.
    try {
      if (outputStream == null) {
        outputStream = Files.newOutputStream(spillPath, StandardOpenOption.TRUNCATE_EXISTING);
      }
      request.readBytes(outputStream, request.readableBytes());
    } catch (IOException e) {
      throw new RuntimeException("Failed to write request to spilling output", e);
    }
  }

  @Override
  public void finished(HttpResponder responder) {
    Closeables.closeQuietly(outputStream);

    try (InputStream is = new CombineInputStream(buffer, outputStream == null ? null : spillPath)) {
      processInput(is, responder);
    } catch (Exception e) {
      Throwables.propagateIfPossible(e);
      throw new RuntimeException(String.format("Failed to process input from buffer%s",
                                               outputStream == null ? "" : " and spill path " + spillPath), e);
    } finally {
      cleanup();
    }
  }

  @Override
  public void handleError(Throwable cause) {
    Closeables.closeQuietly(outputStream);
    cleanup();
  }

  private void cleanup() {
    buffer.release();
    if (spillPath != null) {
      try {
        Files.deleteIfExists(spillPath);
      } catch (IOException e) {
        LOG.warn("Failed to delete file {}", spillPath, e);
      }
    }
  }
}
