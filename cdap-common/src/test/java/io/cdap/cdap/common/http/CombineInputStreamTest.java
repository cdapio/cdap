/*
 * Copyright © 2020 Cask Data, Inc.
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

import com.google.common.io.ByteStreams;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Unit tests for the {@link CombineInputStream}.
 */
public class CombineInputStreamTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testEmpty() throws IOException {
    try (InputStream is = new CombineInputStream(Unpooled.EMPTY_BUFFER, null)) {
      Assert.assertEquals(0, is.available());
      Assert.assertTrue(is.read() < 0);
    }
  }

  @Test
  public void testBufferOnly() throws IOException {
    String msg = "Testing message";
    ByteBuf buffer = Unpooled.buffer();
    buffer.writeBytes(StandardCharsets.UTF_8.encode(msg));
    try (InputStream is = new CombineInputStream(buffer, null)) {
      testReadAndReset(msg, is);
    }
  }

  @Test
  public void testBufferAndFile() throws IOException {
    String msg = "Testing message";
    ByteBuf buffer = Unpooled.buffer();
    buffer.writeBytes(StandardCharsets.UTF_8.encode(msg));
    Path file = TEMP_FOLDER.newFile().toPath();
    Files.write(file, msg.getBytes(StandardCharsets.UTF_8), StandardOpenOption.TRUNCATE_EXISTING);

    try (InputStream is = new CombineInputStream(buffer, file)) {
      testReadAndReset(msg + msg, is);
    }
  }

  private void testReadAndReset(String msg, InputStream is) throws IOException {
    Assert.assertTrue(is.markSupported());
    is.mark(Integer.MAX_VALUE);

    // Read the whole stream
    Assert.assertEquals(msg, new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));

    // Reset and read one byte at a time
    is.reset();
    byte[] bytes = new byte[is.available()];
    int idx = 0;
    int b = is.read();
    while (b >= 0) {
      bytes[idx++] = (byte) b;
      b = is.read();
    }
    Assert.assertEquals(msg, new String(bytes, StandardCharsets.UTF_8));
  }
}
