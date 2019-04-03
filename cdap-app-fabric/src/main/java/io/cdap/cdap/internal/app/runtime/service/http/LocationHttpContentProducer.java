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

package co.cask.cdap.internal.app.runtime.service.http;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.service.http.HttpContentProducer;
import com.google.common.io.Closeables;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * A {@link HttpContentProducer} that produce contents by reading from a {@link Location}.
 */
public class LocationHttpContentProducer extends HttpContentProducer {

  private static final Logger LOG = LoggerFactory.getLogger(LocationHttpContentProducer.class);

  // Default 64K chunk size
  private static final int DEFAULT_CHUNK_SIZE = 65536;

  private final ReadableByteChannel input;
  private final Location location;
  private final int chunkSize;

  public LocationHttpContentProducer(Location location) throws IOException {
    this(location, DEFAULT_CHUNK_SIZE);
  }

  public LocationHttpContentProducer(Location location, int chunkSize) throws IOException {
    this.input = Channels.newChannel(location.getInputStream());
    this.location = location;
    this.chunkSize = chunkSize;
  }

  @Override
  public long getContentLength() {
    try {
      return location.length();
    } catch (IOException e) {
      return -1L;
    }
  }

  @Override
  public ByteBuffer nextChunk(Transactional transactional) throws Exception {
    ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
    if (input.read(buffer) < 0) {
      buffer.position(0).limit(0);
      return buffer;
    }
    buffer.flip();
    return buffer;
  }

  @Override
  @TransactionPolicy(TransactionControl.EXPLICIT)
  public void onFinish() throws Exception {
    input.close();
  }

  @Override
  @TransactionPolicy(TransactionControl.EXPLICIT)
  public void onError(Throwable failureCause) {
    Closeables.closeQuietly(input);
    LOG.warn("Failure in producing http content from location {}", location, failureCause);
  }
}
