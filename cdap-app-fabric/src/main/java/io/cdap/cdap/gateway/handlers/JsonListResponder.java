/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers;

import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;
import io.cdap.http.ChunkResponder;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Given an open http connection on an {@link HttpResponder}, instances of this class can be used to
 * send  a list of Json objects in a streaming fashion, with necessary prefix and suffix. Please use
 * respective child class depending on the prefix and suffix requirement.
 */
public abstract class JsonListResponder {

  protected final HttpResponder responder;
  protected final JsonWriter jsonWriter;
  protected Gson gson;
  protected ChunkResponder chunkResponder;
  protected boolean firstObjectReceived;

  /**
   * Construct an instance of JsonListResponder
   *
   * @param gson {@link Gson}
   * @param responder instance of {@link HttpResponder} which will be used to send Json objects
   *     in streaming fashion
   */
  protected JsonListResponder(
      Gson gson, HttpResponder responder) {
    this.gson = gson;
    this.responder = responder;
    this.firstObjectReceived = false;
    jsonWriter = new JsonWriter(new Writer() {
      @Override
      public void write(char[] cbuf, int off, int len) throws IOException {
        CharBuffer charBuffer = CharBuffer.wrap(cbuf, off, len);
        ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(charBuffer);
        chunkResponder.sendChunk(byteBuffer);
      }

      @Override
      public void flush() throws IOException {
        chunkResponder.flush();
      }

      @Override
      public void close() throws IOException {
        chunkResponder.close();
      }
    });
  }

  /**
   * Write the value as json to {@link HttpResponder}
   *
   * @param value the object being sent
   */
  public void send(Object value) {
    try {
      prepareWrite();
      gson.toJson(value, value.getClass(), jsonWriter);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private void prepareWrite() throws IOException {
    if (!firstObjectReceived) {
      firstObjectReceived = true;
      chunkResponder = responder.sendChunkStart(HttpResponseStatus.OK);
      startResponse();
    }
  }

  /**
   * Prepare the header in the response and start an array for the following objects.
   */
  protected abstract void startResponse() throws IOException;

  /**
   * Perform cleanup operations
   */
  public void finish() throws IOException {
    prepareWrite();
    finishResponse();
    jsonWriter.close();
  }

  /**
   * Write the footer in the response and end any arrays and any objects created in the
   * startResponse.
   */
  protected abstract void finishResponse() throws IOException;
}
