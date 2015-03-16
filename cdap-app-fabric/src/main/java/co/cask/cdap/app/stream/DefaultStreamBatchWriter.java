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

package co.cask.cdap.app.stream;

import co.cask.cdap.api.data.stream.StreamBatchWriter;
import co.cask.cdap.common.io.ByteBuffers;
import co.cask.cdap.proto.Id;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;

/**
 * Implementation of {@link StreamBatchWriter}.
 */
public class DefaultStreamBatchWriter implements StreamBatchWriter {

  private final HttpURLConnection connection;
  private final OutputStream outputStream;
  private final Id.Stream stream;
  private boolean open;

  public DefaultStreamBatchWriter(HttpURLConnection connection, Id.Stream stream) throws IOException {
    this.connection = connection;
    this.outputStream = connection.getOutputStream();
    this.stream = stream;
    this.open = true;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public int write(ByteBuffer data) throws IOException {
    int size = data.remaining();
    ByteBuffers.writeToStream(data, outputStream);
    return size;
  }

  @Override
  public void close() throws IOException {
    int responseCode;
    try {
      open = false;
      outputStream.close();
      responseCode = connection.getResponseCode();
    } finally {
      connection.disconnect();
    }

    if (responseCode == HttpResponseStatus.NOT_FOUND.code()) {
      throw new IOException(String.format("Stream %s not found", stream));
    }

    if (responseCode < 200 || responseCode >= 300) {
      throw new IOException(String.format("Writing to Stream %s did not succeed. Stream Service ResponseCode : %d",
                                          stream, responseCode));
    }
  }
}
