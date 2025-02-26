/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.storage.spanner.compression;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link SnappyCompressor}.
 */
public class SnappyCompressorTest {

  private final SnappyCompressor compressor = new SnappyCompressor();

  @Test
  public void testStringCompression() throws IOException {
    String data = "This is a test string for Snappy compression.";
    String compressed = compressor.compress(data);
    String decompressed = compressor.decompress(compressed);
    Assert.assertEquals(data, decompressed);
  }

  @Test
  public void testStringCompression_EmptyString() throws IOException {
    String data = "";
    String compressed = compressor.compress(data);
    String decompressed = compressor.decompress(compressed);
    Assert.assertEquals(data, decompressed);
  }

  @Test(expected = IOException.class)
  public void testStringDecompression_EmptyString() throws IOException {
    String data = "";
    compressor.decompress(data);
  }

  @Test(expected = NullPointerException.class)
  public void testStringCompression_NullString() throws IOException {
    String data = null;
    compressor.compress(data);
  }

  @Test(expected = NullPointerException.class)
  public void testStringDecompression_NullString() throws IOException {
    String data = null;
    compressor.decompress(data);
  }

  @Test
  public void testBytesCompression() throws IOException {
    byte[] data = "This is a test byte array for Snappy compression.".getBytes(
        StandardCharsets.UTF_8);
    byte[] compressed = compressor.compress(data);
    byte[] decompressed = compressor.decompress(compressed);
    Assert.assertArrayEquals(data, decompressed);
  }

  @Test(expected = NullPointerException.class)
  public void testBytesCompression_NullBytes() throws IOException {
    byte[] data = null;
    compressor.compress(data);
  }

  @Test(expected = NullPointerException.class)
  public void testBytesDecompression_NullBytes() throws IOException {
    byte[] data = null;
    compressor.decompress(data);
  }

  @Test(expected = IOException.class)
  public void testInvalidCompressedData() throws IOException {
    byte[] invalidData = new byte[0];
    compressor.decompress(invalidData);
  }
}
