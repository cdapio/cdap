/*
 * Copyright © 2024 Cask Data, Inc.
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

import io.netty.buffer.ByteBuf;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link LocationBodyProducer}.
 */
public class LocationBodyProducerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testStreamingAndFinished() throws Exception {
    LocalLocationFactory lf = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    Location location = lf.create("payload.txt");
    String content = "LocationBodyProducer test payload";
    try (OutputStream os = location.getOutputStream()) {
      os.write(content.getBytes(StandardCharsets.UTF_8));
    }

    LocationBodyProducer producer = new LocationBodyProducer(location);
    ByteArrayOutputStream received = new ByteArrayOutputStream();
    while (true) {
      ByteBuf chunk = producer.nextChunk();
      try {
        if (!chunk.isReadable()) {
          break;
        }
        chunk.readBytes(received, chunk.readableBytes());
      } finally {
        chunk.release();
      }
    }
    producer.finished();
    Assert.assertEquals(content, new String(received.toByteArray(), StandardCharsets.UTF_8));

    // Calling finished() when nextChunk() was never called should also be safe.
    LocationBodyProducer unusedProducer = new LocationBodyProducer(location);
    unusedProducer.finished();
  }

  @Test
  public void testHandleErrorWithNullInputStreamAndCloseFailure() throws Exception {
    Location mockLocation = Mockito.mock(Location.class);

    // 1. handleError before nextChunk() when inputStream is null (both non-null and null cause).
    LocationBodyProducer unstartedProducer = new LocationBodyProducer(mockLocation);
    unstartedProducer.handleError(new IOException("before open"));
    unstartedProducer.handleError(null);

    // 2. handleError when inputStream.close() throws IOException.
    AtomicBoolean closeAttempted = new AtomicBoolean(false);
    InputStream failingCloseStream = new ByteArrayInputStream("abc".getBytes(StandardCharsets.UTF_8)) {
      @Override
      public void close() throws IOException {
        closeAttempted.set(true);
        throw new IOException("close failure");
      }
    };
    Mockito.when(mockLocation.getInputStream()).thenReturn(failingCloseStream);

    LocationBodyProducer producer = new LocationBodyProducer(mockLocation);
    ByteBuf chunk = producer.nextChunk();
    chunk.release();

    // Must swallow IOException from inputStream.close() without throwing.
    producer.handleError(new RuntimeException("transfer aborted"));
    Assert.assertTrue(closeAttempted.get());
  }
}
