/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.service.http;

import io.cdap.cdap.common.io.Locations;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link LocationHttpContentProducer}.
 */
public class LocationHttpContentProducerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testReadChunksAndFinishClosesInput() throws Exception {
    File file = TEMP_FOLDER.newFile();
    byte[] payload = "0123456789abcdef".getBytes(StandardCharsets.UTF_8);
    Files.write(file.toPath(), payload);

    Location location = Locations.toLocation(file);
    LocationHttpContentProducer producer = new LocationHttpContentProducer(location, 5);
    Assert.assertEquals(payload.length, producer.getContentLength());

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    while (true) {
      ByteBuffer chunk = producer.nextChunk(null);
      if (!chunk.hasRemaining()) {
        break;
      }
      byte[] bytes = new byte[chunk.remaining()];
      chunk.get(bytes);
      out.write(bytes);
    }

    Assert.assertArrayEquals(payload, out.toByteArray());
    producer.onFinish();
  }

  @Test
  public void testOnErrorClosesInputQuietlyEvenWhenCloseThrows() throws Exception {
    AtomicBoolean closeCalled = new AtomicBoolean(false);
    InputStream failingCloseStream = new ByteArrayInputStream("abc".getBytes(StandardCharsets.UTF_8)) {
      @Override
      public void close() throws IOException {
        closeCalled.set(true);
        throw new IOException("close failure");
      }
    };

    Location location = Mockito.mock(Location.class);
    Mockito.when(location.getInputStream()).thenReturn(failingCloseStream);
    Mockito.when(location.length()).thenThrow(new IOException("length failure"));

    LocationHttpContentProducer producer = new LocationHttpContentProducer(location);
    Assert.assertEquals(-1L, producer.getContentLength());

    // Must close quietly without propagating the close IOException.
    producer.onError(new RuntimeException("stream aborted"));
    Assert.assertTrue(closeCalled.get());
  }
}
