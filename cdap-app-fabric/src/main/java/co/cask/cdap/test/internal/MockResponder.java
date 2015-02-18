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
package co.cask.cdap.test.internal;

import co.cask.http.AbstractHttpResponder;
import co.cask.http.ChunkResponder;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

/**
 * A mock implementation of {@link HttpResponder} that only record the response status.
 */
public final class MockResponder extends AbstractHttpResponder {
  private HttpResponseStatus status = null;
  private ChannelBuffer content = null;
  private static final Gson GSON = new Gson();


  public HttpResponseStatus getStatus() {
    return status;
  }

  public <T> T decodeResponseContent(TypeToken<T> type) {
    return decodeResponseContent(type, GSON);
  }

  public <T> T decodeResponseContent(TypeToken<T> type, Gson gson) {
    JsonReader jsonReader = new JsonReader(new InputStreamReader
                                             (new ChannelBufferInputStream(content), Charsets.UTF_8));
    return gson.fromJson(jsonReader, type.getType());
  }

  @Override
  public ChunkResponder sendChunkStart(HttpResponseStatus status, Multimap<String, String> headers) {
    this.status = status;
    return new ChunkResponder() {
      @Override
      public void sendChunk(ByteBuffer chunk) throws IOException {
        // No-op
      }

      @Override
      public void sendChunk(ChannelBuffer chunk) throws IOException {
        // No-op
      }

      @Override
      public void close() throws IOException {
        // No-op
      }
    };
  }

  @Override
  public void sendContent(HttpResponseStatus status,
                          ChannelBuffer content, String contentType, Multimap<String, String> headers) {
    if (content != null) {
      this.content = content;
    }
    this.status = status;
  }

  @Override
  public void sendFile(File file, Multimap<String, String> headers) {
    this.status = HttpResponseStatus.OK;
  }
}
