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

package co.cask.cdap.logging.gateway.handlers;

import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.logging.read.LogEvent;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpHeaders;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

/**
 * LogReader BodyProducer that serves log events as JSON objects.
 */
public abstract class AbstractJSONLogProducer extends AbstractChunkedLogProducer {

  protected static final Gson GSON = new Gson();

  private final ChannelBuffer channelBuffer;
  private final JsonWriter jsonWriter;

  private boolean hasStarted = false;


  AbstractJSONLogProducer(CloseableIterator<LogEvent> logEventIter) {
    super(logEventIter);
    this.channelBuffer = ChannelBuffers.dynamicBuffer(BUFFER_BYTES);
    this.jsonWriter = new JsonWriter(new OutputStreamWriter(new ChannelBufferOutputStream(channelBuffer),
                                                            StandardCharsets.UTF_8));
  }

  @Override
  protected Multimap<String, String> getResponseHeaders() {
    return ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "application/json");
  }

  @Override
  protected ChannelBuffer onWriteStart() throws IOException {
    channelBuffer.clear();
    jsonWriter.beginArray();
    jsonWriter.flush();
    return channelBuffer;
  }

  @Override
  protected ChannelBuffer writeLogEvents(CloseableIterator<LogEvent> logEventIter) throws IOException {
    channelBuffer.clear();

    while (logEventIter.hasNext() && channelBuffer.readableBytes() < BUFFER_BYTES) {
      Object encodedObject = encodeSend(logEventIter.next());
      GSON.toJson(encodedObject, encodedObject.getClass(), jsonWriter);
      jsonWriter.flush();
    }
    return channelBuffer;
  }

  @Override
  protected ChannelBuffer onWriteFinish() throws IOException {
    channelBuffer.clear();
    jsonWriter.endArray();
    jsonWriter.flush();
    return channelBuffer;
  }

  /**
   * Return a {@link Object} that will be serialized to a JSON string
   */
  protected abstract Object encodeSend(LogEvent logEvent);
}
