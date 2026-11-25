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

package io.cdap.cdap.storage.spanner;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Struct;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.storage.spanner.compression.CompressionConfig;
import io.cdap.cdap.storage.spanner.compression.CompressorType;
import io.cdap.cdap.storage.spanner.compression.SnappyCompressor;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link SpannerStructuredRow} compression and error propagation.
 */
public class SpannerStructuredRowTest {

  @Test
  public void testGetStringAndGetBytesDecompressSuccessAndUncompressed() throws Exception {
    SpannerStructuredTableSchema schema = createSchema();
    SnappyCompressor snappy = new SnappyCompressor();

    String rawString = "hello compressed spanner row payload";
    byte[] rawBytes = "binary compressed spanner payload".getBytes(StandardCharsets.UTF_8);

    Struct struct = Struct.newBuilder()
        .set("id").to("row-1")
        .set("str_col").to(snappy.compress(rawString))
        .set("str_col" + CompressionConfig.COMPRESSED_COLUMN_SUFFIX).to("SNAPPY")
        .set("bytes_col").to(ByteArray.copyFrom(snappy.compress(rawBytes)))
        .set("bytes_col" + CompressionConfig.COMPRESSED_COLUMN_SUFFIX).to("SNAPPY")
        .set("plain_col").to("plain-text")
        .build();

    SpannerStructuredRow row = new SpannerStructuredRow(schema, struct);
    Assert.assertEquals(rawString, row.getString("str_col"));
    Assert.assertArrayEquals(rawBytes, row.getBytes("bytes_col"));
    Assert.assertEquals("plain-text", row.getString("plain_col"));
    Assert.assertNull(row.getString("missing_col"));
    Assert.assertEquals(
        Collections.singletonList(Fields.stringField("id", "row-1")),
        row.getPrimaryKeys());
  }

  @Test
  public void testGetStringAndGetBytesWrapDecompressionIOException() {
    SpannerStructuredTableSchema schema = createSchema();
    byte[] invalidSnappy = new byte[]{(byte) 0xFF, 0x01, 0x02, 0x03, 0x04};
    String corruptBase64 = Base64.getEncoder().encodeToString(invalidSnappy);

    Struct struct = Struct.newBuilder()
        .set("id").to("row-bad")
        .set("str_col").to(corruptBase64)
        .set("str_col" + CompressionConfig.COMPRESSED_COLUMN_SUFFIX).to("SNAPPY")
        .set("bytes_col").to(ByteArray.copyFrom(invalidSnappy))
        .set("bytes_col" + CompressionConfig.COMPRESSED_COLUMN_SUFFIX).to("SNAPPY")
        .build();

    SpannerStructuredRow row = new SpannerStructuredRow(schema, struct);

    try {
      row.getString("str_col");
      Assert.fail("Expected RuntimeException wrapping IOException on corrupt string decompression");
    } catch (RuntimeException actual) {
      Assert.assertTrue(
          "Expected IOException cause, got: " + actual.getCause(),
          actual.getCause() instanceof IOException);
    }

    try {
      row.getBytes("bytes_col");
      Assert.fail("Expected RuntimeException wrapping IOException on corrupt bytes decompression");
    } catch (RuntimeException actual) {
      Assert.assertTrue(
          "Expected IOException cause, got: " + actual.getCause(),
          actual.getCause() instanceof IOException);
    }
  }

  private static SpannerStructuredTableSchema createSchema() {
    Map<String, CompressionConfig> compressionConfigs = new HashMap<>();
    compressionConfigs.put("str_col", new CompressionConfig(CompressorType.SNAPPY));
    compressionConfigs.put("bytes_col", new CompressionConfig(CompressorType.SNAPPY));

    return new SpannerStructuredTableSchema(
        new StructuredTableId("test_table"),
        Arrays.asList(
            Fields.stringType("id"),
            Fields.stringType("str_col"),
            Fields.bytesType("bytes_col"),
            Fields.stringType("plain_col")),
        Collections.singletonList("id"),
        Collections.emptyList(),
        compressionConfigs);
  }
}
