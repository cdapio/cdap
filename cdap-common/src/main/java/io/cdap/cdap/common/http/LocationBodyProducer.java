/*
 * Copyright © 2021 Cask Data, Inc.
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

import com.google.common.io.Closeables;
import io.cdap.http.BodyProducer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.InputStream;
import javax.annotation.Nullable;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BodyProducer} for streaming content from a {@link Location}.
 */
public class LocationBodyProducer extends BodyProducer {

  private static final Logger LOG = LoggerFactory.getLogger(LocationBodyProducer.class);
  private static final int CHUNK_SIZE = 64 * 1024;  // 64K

  private final Location location;
  private InputStream inputStream;

  public LocationBodyProducer(Location location) {
    this.location = location;
  }

  @Override
  public ByteBuf nextChunk() throws Exception {
    if (inputStream == null) {
      inputStream = location.getInputStream();
    }

    ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer(CHUNK_SIZE);
    buffer.writeBytes(inputStream, buffer.writableBytes());

    return buffer;
  }

  @Override
  public void finished() throws Exception {
    if (inputStream != null) {
      inputStream.close();
    }
  }

  @Override
  public void handleError(@Nullable Throwable throwable) {
    if (throwable != null) {
      LOG.warn("Error in sending location {}", location, throwable);
    }
    Closeables.closeQuietly(inputStream);
  }
}
