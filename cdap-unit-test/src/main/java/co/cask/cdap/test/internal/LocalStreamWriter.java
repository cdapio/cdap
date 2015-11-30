/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.data.stream.StreamBatchWriter;
import co.cask.cdap.api.data.stream.StreamWriter;
import co.cask.cdap.api.stream.StreamEventData;
import co.cask.cdap.proto.Id;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Implements {@link StreamWriter} to enable you to write to streams in tests without making HTTP requests
 */
public class LocalStreamWriter implements StreamWriter {

  private final StreamManagerFactory streamManagerFactory;
  private final Id.Namespace namespace;

  @Inject
  public LocalStreamWriter(@Assisted("run") Id.Run run, StreamManagerFactory streamManagerFactory) {
    this.namespace = run.getNamespace();
    this.streamManagerFactory = streamManagerFactory;
  }

  @Override
  public void write(String stream, String data) throws IOException {
    streamManagerFactory.create(Id.Stream.from(namespace, stream)).send(data);
  }

  @Override
  public void write(String stream, String data, Map<String, String> headers) throws IOException {
    streamManagerFactory.create(Id.Stream.from(namespace, stream)).send(headers, data);
  }

  @Override
  public void write(String stream, ByteBuffer data) throws IOException {
    streamManagerFactory.create(Id.Stream.from(namespace, stream)).send(data);
  }

  @Override
  public void write(String stream, StreamEventData data) throws IOException {
    streamManagerFactory.create(Id.Stream.from(namespace, stream))
      .send(data.getHeaders(), data.getBody());
  }

  @Override
  public void writeFile(String stream, File file, String contentType) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public StreamBatchWriter createBatchWriter(String stream, String contentType) throws IOException {
    return null;
  }
}
