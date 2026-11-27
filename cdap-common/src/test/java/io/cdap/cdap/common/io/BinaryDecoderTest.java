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

package io.cdap.cdap.common.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link BinaryDecoder}.
 */
public class BinaryDecoderTest {

  @Test
  public void testReadEmptyAndSmallBytesAndString() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = new BinaryEncoder(out);
    encoder.writeBytes(new byte[0]);
    encoder.writeString("");
    encoder.writeString("hello-cdap");
    byte[] small = new byte[128];
    new Random(42L).nextBytes(small);
    encoder.writeBytes(small);
    encoder.writeBytes(new byte[0]);
    encoder.writeBytes(small);
    encoder.writeString("after-skip");

    BinaryDecoder decoder = new BinaryDecoder(new ByteArrayInputStream(out.toByteArray()));
    Assert.assertEquals(0, decoder.readBytes().remaining());
    Assert.assertEquals("", decoder.readString());
    Assert.assertEquals("hello-cdap", decoder.readString());
    Assert.assertArrayEquals(small, toArray(decoder.readBytes()));
    // Zero-length and positive-length skipBytes() must succeed.
    decoder.skipBytes();
    decoder.skipBytes();
    Assert.assertEquals("after-skip", decoder.readString());
  }

  @Test
  public void testReadBoundaryAndLargeBytesAcrossMultipleChunks() throws IOException {
    Random random = new Random(123L);
    // Cover exact 8 KiB initial buffer, 8 KiB + 1, 16 KiB, and > 25 KiB multi-growth reads.
    for (int size : new int[]{8192, 8193, 16384, 25 * 1024 + 37}) {
      byte[] expected = new byte[size];
      random.nextBytes(expected);

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      new BinaryEncoder(out).writeBytes(expected);

      FilterInputStream chunkedInput = new FilterInputStream(
          new ByteArrayInputStream(out.toByteArray())) {
        @Override
        public int read(byte[] b, int off, int len) throws IOException {
          return super.read(b, off, Math.min(len, 1024));
        }
      };

      Assert.assertArrayEquals("Mismatch for size " + size,
          expected, toArray(new BinaryDecoder(chunkedInput).readBytes()));
    }

    // Large multi-byte UTF-8 string crossing the 8 KiB buffer boundary.
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 4000; i++) {
      sb.append("café-");
    }
    String expectedUtf8 = sb.toString();
    ByteArrayOutputStream strOut = new ByteArrayOutputStream();
    new BinaryEncoder(strOut).writeString(expectedUtf8);
    Assert.assertEquals(expectedUtf8,
        new BinaryDecoder(new ByteArrayInputStream(strOut.toByteArray())).readString());
  }

  @Test
  public void testNegativeByteLengthThrowsIoException() throws IOException {
    for (int negativeLength : new int[]{-1, -100, Integer.MIN_VALUE}) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      new BinaryEncoder(out).writeInt(negativeLength);
      byte[] encoded = out.toByteArray();

      assertNegativeLengthRejected("readBytes", negativeLength,
          () -> new BinaryDecoder(new ByteArrayInputStream(encoded)).readBytes());
      assertNegativeLengthRejected("readString", negativeLength,
          () -> new BinaryDecoder(new ByteArrayInputStream(encoded)).readString());
      assertNegativeLengthRejected("skipBytes", negativeLength,
          () -> new BinaryDecoder(new ByteArrayInputStream(encoded)).skipBytes());
    }
  }

  private interface DecoderAction {
    void run() throws IOException;
  }

  private static void assertNegativeLengthRejected(String operation, int negativeLength,
      DecoderAction action) {
    try {
      action.run();
      Assert.fail("Expected IOException from " + operation
          + " for negative length " + negativeLength);
    } catch (IOException expected) {
      Assert.assertFalse("Expected non-EOF IOException from " + operation
          + ", got: " + expected, expected instanceof EOFException);
      Assert.assertTrue("Expected message mentioning negative length from " + operation
          + ", got: " + expected.getMessage(),
          expected.getMessage() != null && expected.getMessage().contains("negative"));
    }
  }

  @Test
  public void testTruncatedLargeLengthThrowsEofWithBoundedAllocation() throws IOException {
    for (int declaredLength : new int[]{1 << 30, Integer.MAX_VALUE}) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      new BinaryEncoder(out).writeInt(declaredLength);
      AtomicInteger maxBuf = new AtomicInteger();
      FilterInputStream observing = new FilterInputStream(
          new ByteArrayInputStream(out.toByteArray())) {
        @Override
        public int read(byte[] b, int off, int len) throws IOException {
          maxBuf.accumulateAndGet(b.length, Math::max);
          return super.read(b, off, len);
        }
      };
      try {
        new BinaryDecoder(observing).readBytes();
        Assert.fail("Expected EOFException for declared length " + declaredLength);
      } catch (EOFException expected) {
        // expected
      }
      Assert.assertTrue("Allocated buffer exceeded 8192: " + maxBuf.get(), maxBuf.get() <= 8192);
    }

    // Truncated mid-read after growing past the 8 KiB initial buffer.
    ByteArrayOutputStream partialOut = new ByteArrayOutputStream();
    new BinaryEncoder(partialOut).writeInt(20_000);
    partialOut.write(new byte[10_000]);
    try {
      new BinaryDecoder(new ByteArrayInputStream(partialOut.toByteArray())).readBytes();
      Assert.fail("Expected EOFException for mid-read truncation");
    } catch (EOFException expected) {
      // expected
    }
  }

  private static byte[] toArray(ByteBuffer buffer) {
    byte[] result = new byte[buffer.remaining()];
    buffer.get(result);
    return result;
  }
}
