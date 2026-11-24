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

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import io.cdap.cdap.spi.data.compression.Compressor;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.storage.spanner.compression.CompressionConfig;
import io.cdap.cdap.storage.spanner.compression.CompressorFactory;
import io.cdap.cdap.storage.spanner.compression.CompressorType;
import io.cdap.cdap.storage.spanner.compression.SnappyCompressor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link SpannerStructuredTable} compression and {@link IOException} propagation.
 */
public class SpannerStructuredTableCompressionTest {

  private static final int LARGE_STRING_LEN = 2621441;
  private static final int LARGE_BYTES_LEN = 10 * 1024 * 1024 + 1;

  @Test
  public void testUpsertCompressesLargeStringAndBytesFields() throws Exception {
    AtomicReference<Statement> lastStatement = new AtomicReference<>();
    TransactionContext txContext = createStubTransactionContext(lastStatement);
    SpannerStructuredTableSchema schema = createSchema();

    char[] chars = new char[LARGE_STRING_LEN];
    Arrays.fill(chars, 'a');
    String largeString = new String(chars);
    byte[] largeBytes = new byte[LARGE_BYTES_LEN];
    Arrays.fill(largeBytes, (byte) 7);

    try (SpannerStructuredTable table = new SpannerStructuredTable(txContext, schema, null)) {
      table.upsert(Arrays.asList(
          Fields.stringField("id", "k1"),
          Fields.stringField("big_str", largeString),
          Fields.bytesField("big_bytes", largeBytes)));
    }

    Statement executed = lastStatement.get();
    Assert.assertNotNull(executed);
    SnappyCompressor snappy = new SnappyCompressor();
    String compressedStr = executed.getParameters().get("big_str").getString();
    byte[] compressedBytes = executed.getParameters().get("big_bytes").getBytes().toByteArray();
    Assert.assertEquals(largeString, snappy.decompress(compressedStr));
    Assert.assertArrayEquals(largeBytes, snappy.decompress(compressedBytes));
    Assert.assertEquals(
        "SNAPPY",
        executed.getParameters()
            .get("big_str" + CompressionConfig.COMPRESSED_COLUMN_SUFFIX).getString());
    Assert.assertEquals(
        "SNAPPY",
        executed.getParameters()
            .get("big_bytes" + CompressionConfig.COMPRESSED_COLUMN_SUFFIX).getString());
  }

  @Test
  public void testUpsertWrapsCompressorIOExceptionForStringAndBytes() throws Exception {
    AtomicReference<Statement> lastStatement = new AtomicReference<>();
    TransactionContext txContext = createStubTransactionContext(lastStatement);
    SpannerStructuredTableSchema schema = createSchema();

    char[] chars = new char[LARGE_STRING_LEN];
    Arrays.fill(chars, 'x');
    String largeString = new String(chars);
    byte[] largeBytes = new byte[LARGE_BYTES_LEN];

    IOException compressFailure = new IOException("simulated compression failure");
    Compressor failingCompressor = new Compressor() {
      @Nonnull
      @Override
      public String compress(@Nonnull String data) throws IOException {
        throw compressFailure;
      }

      @Nonnull
      @Override
      public byte[] compress(@Nonnull byte[] data) throws IOException {
        throw compressFailure;
      }

      @Nonnull
      @Override
      public String decompress(@Nonnull String compressedData) throws IOException {
        throw compressFailure;
      }

      @Nonnull
      @Override
      public byte[] decompress(@Nonnull byte[] compressedData) throws IOException {
        throw compressFailure;
      }
    };

    Field compressorsField = CompressorFactory.class.getDeclaredField("compressors");
    compressorsField.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(compressorsField, compressorsField.getModifiers() & ~Modifier.FINAL);

    Object originalMap = compressorsField.get(null);
    try {
      compressorsField.set(
          null, Collections.singletonMap(CompressorType.SNAPPY, failingCompressor));

      try (SpannerStructuredTable table = new SpannerStructuredTable(txContext, schema, null)) {
        try {
          table.upsert(Arrays.asList(
              Fields.stringField("id", "k1"),
              Fields.stringField("big_str", largeString)));
          Assert.fail("Expected RuntimeException wrapping IOException for string compression");
        } catch (RuntimeException actual) {
          Assert.assertSame(compressFailure, actual.getCause());
        }

        try {
          table.upsert(Arrays.asList(
              Fields.stringField("id", "k2"),
              Fields.bytesField("big_bytes", largeBytes)));
          Assert.fail("Expected RuntimeException wrapping IOException for bytes compression");
        } catch (RuntimeException actual) {
          Assert.assertSame(compressFailure, actual.getCause());
        }
      }
    } finally {
      compressorsField.set(null, originalMap);
    }
  }

  private static TransactionContext createStubTransactionContext(
      AtomicReference<Statement> capturedStatement) {
    return (TransactionContext) Proxy.newProxyInstance(
        TransactionContext.class.getClassLoader(),
        new Class<?>[]{TransactionContext.class},
        (proxy, method, args) -> {
          String name = method.getName();
          if ("readRow".equals(name)) {
            return null;
          }
          if ("executeUpdate".equals(name)) {
            capturedStatement.set((Statement) args[0]);
            return 1L;
          }
          return null;
        });
  }

  private static SpannerStructuredTableSchema createSchema() {
    Map<String, CompressionConfig> compressionConfigs = new HashMap<>();
    compressionConfigs.put("big_str", new CompressionConfig(CompressorType.SNAPPY));
    compressionConfigs.put("big_bytes", new CompressionConfig(CompressorType.SNAPPY));

    return new SpannerStructuredTableSchema(
        new StructuredTableId("spanner_comp_test"),
        Arrays.asList(
            Fields.stringType("id"),
            Fields.stringType("big_str"),
            Fields.bytesType("big_bytes")),
        Collections.singletonList("id"),
        Collections.emptyList(),
        compressionConfigs);
  }
}
