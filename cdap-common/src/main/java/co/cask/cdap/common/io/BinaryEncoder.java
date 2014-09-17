/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.common.io;

import com.google.common.base.Charsets;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 *  An {@link Encoder} for binary-format data.
 */
public final class BinaryEncoder implements Encoder {

  private final OutputStream output;

  public BinaryEncoder(OutputStream output) {
    this.output = output;
  }

  @Override
  public Encoder writeNull() throws IOException {
    // No-op
    return this;
  }

  @Override
  public Encoder writeBool(boolean b) throws IOException {
    output.write(b ? 1 : 0);
    return this;
  }

  @Override
  public Encoder writeInt(int i) throws IOException {
    // Compute the zig-zag value. First double the value and flip the bit if the input is negative.
    int val = (i << 1) ^ (i >> 31);

    if ((val & ~0x7f) != 0) {
      output.write(0x80 | val & 0x7f);
      val >>>= 7;
      while (val > 0x7f) {
        output.write(0x80 | val & 0x7f);
        val >>>= 7;
      }
    }
    output.write(val);

    return this;
  }

  @Override
  public Encoder writeLong(long l) throws IOException {
    // Compute the zig-zag value. First double the value and flip the bit if the input is negative.
    long val = (l << 1) ^ (l >> 63);

    if ((val & ~0x7f) != 0) {
      output.write((int) (0x80 | val & 0x7f));
      val >>>= 7;
      while (val > 0x7f) {
        output.write((int) (0x80 | val & 0x7f));
        val >>>= 7;
      }
    }
    output.write((int) val);

    return this;
  }

  @Override
  public Encoder writeFloat(float f) throws IOException {
    int bits = Float.floatToIntBits(f);
    output.write(bits & 0xff);
    output.write((bits >> 8) & 0xff);
    output.write((bits >> 16) & 0xff);
    output.write((bits >> 24) & 0xff);
    return this;
  }

  @Override
  public Encoder writeDouble(double d) throws IOException {
    long bits = Double.doubleToLongBits(d);
    int low = (int) bits;
    int high = (int) (bits >> 32);

    output.write(low & 0xff);
    output.write((low >> 8) & 0xff);
    output.write((low >> 16) & 0xff);
    output.write((low >> 24) & 0xff);

    output.write(high & 0xff);
    output.write((high >> 8) & 0xff);
    output.write((high >> 16) & 0xff);
    output.write((high >> 24) & 0xff);

    return this;
  }

  @Override
  public Encoder writeString(String s) throws IOException {
    return writeBytes(Charsets.UTF_8.encode(s));
  }

  @Override
  public Encoder writeBytes(byte[] bytes) throws IOException {
    return writeBytes(bytes, 0, bytes.length);
  }

  @Override
  public Encoder writeBytes(byte[] bytes, int off, int len) throws IOException {
    writeLong(len);
    output.write(bytes, off, len);
    return this;
  }

  @Override
  public Encoder writeBytes(ByteBuffer buffer) throws IOException {
    writeInt(buffer.remaining());
    if (buffer.hasArray()) {
      output.write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
    } else {
      byte[] bytes = new byte[buffer.remaining()];
      int pos = buffer.position();
      buffer.get(bytes);
      output.write(bytes);
      buffer.position(pos);
    }

    return this;
  }
}
