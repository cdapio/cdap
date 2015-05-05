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

import co.cask.cdap.proto.Id;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.StreamWriter;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @deprecated Use {@link DefaultStreamManager} instead
 */
@Deprecated
public final class DefaultStreamWriter implements StreamWriter {

  private final StreamManager streamManager;

  @Inject
  public DefaultStreamWriter(StreamManagerFactory streamManagerFactory,
                             @Assisted Id.Stream streamId) throws IOException {
    this.streamManager = streamManagerFactory.create(streamId);
  }

  @Override
  public void createStream() throws IOException {
    streamManager.createStream();
  }

  @Override
  public void send(String content) throws IOException {
    send(Charsets.UTF_8.encode(content));
  }

  @Override
  public void send(byte[] content) throws IOException {
    send(content, 0, content.length);
  }

  @Override
  public void send(byte[] content, int off, int len) throws IOException {
    send(ByteBuffer.wrap(content, off, len));
  }

  @Override
  public void send(ByteBuffer buffer) throws IOException {
    send(ImmutableMap.<String, String>of(), buffer);
  }

  @Override
  public void send(Map<String, String> headers, String content) throws IOException {
    send(headers, Charsets.UTF_8.encode(content));
  }

  @Override
  public void send(Map<String, String> headers, byte[] content) throws IOException {
    send(headers, content, 0, content.length);
  }

  @Override
  public void send(Map<String, String> headers, byte[] content, int off, int len) throws IOException {
    send(headers, ByteBuffer.wrap(content, off, len));
  }

  @Override
  public void send(Map<String, String> headers, ByteBuffer buffer) throws IOException {
    streamManager.send(headers, buffer);
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
  }
}
