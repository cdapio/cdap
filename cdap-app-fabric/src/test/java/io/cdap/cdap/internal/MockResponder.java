/*
 * Copyright © 2014-2019 Cask Data, Inc.
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
package io.cdap.cdap.internal;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import io.cdap.http.AbstractHttpResponder;
import io.cdap.http.BodyProducer;
import io.cdap.http.ChunkResponder;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * A mock implementation of {@link HttpResponder} that only record the response status.
 */
public final class MockResponder extends AbstractHttpResponder {

  private static final Gson GSON = new Gson();

  private HttpResponseStatus status;
  private ByteBuf content;

  public HttpResponseStatus getStatus() {
    return status;
  }

  public String getResponseContentAsString() {
    return content.toString(StandardCharsets.UTF_8);
  }

  public <T> T decodeResponseContent(Type type) {
    return decodeResponseContent(type, GSON);
  }

  public <T> T decodeResponseContent(Type type, Gson gson) {
    JsonReader jsonReader = new JsonReader(new InputStreamReader
                                             (new ByteBufInputStream(content), StandardCharsets.UTF_8));
    return gson.fromJson(jsonReader, type);
  }

  @Override
  public ChunkResponder sendChunkStart(HttpResponseStatus status, HttpHeaders headers) {
    this.status = status;
    return new ChunkResponder() {
      @Override
      public void sendChunk(ByteBuffer chunk) throws IOException {
        sendChunk(Unpooled.wrappedBuffer(chunk));
      }

      @Override
      public void sendChunk(ByteBuf chunk) throws IOException {
        if (content == null) {
          content = Unpooled.buffer(chunk.readableBytes());
        }
        content.writeBytes(chunk);
      }

      @Override
      public void close() throws IOException {
        // No-op
      }
    };
  }

  @Override
  public void sendContent(HttpResponseStatus status, ByteBuf content, HttpHeaders headers) {
    this.content = content.copy();
    this.status = status;
  }

  @Override
  public void sendFile(File file, HttpHeaders headers) {
    this.status = HttpResponseStatus.OK;
  }

  @Override
  public void sendContent(HttpResponseStatus httpResponseStatus, BodyProducer bodyProducer, HttpHeaders headers) {
    this.status = HttpResponseStatus.OK;
  }
}
