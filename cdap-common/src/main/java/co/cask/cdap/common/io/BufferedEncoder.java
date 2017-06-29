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

import com.google.common.base.Function;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * A {@link Encoder} that performs all writes to in memory buffer.
 */
public final class BufferedEncoder implements Encoder {

  private final Encoder encoder;
  private final ByteArrayOutputStream output;

  public BufferedEncoder(int size, Function<OutputStream, Encoder> encoderFactory) {
    output = new ByteArrayOutputStream(size);
    encoder = encoderFactory.apply(output);
  }

  public void reset() {
    output.reset();
  }

  /**
   * Writes all the buffered bytes into the given OutputStream. If the write completed successfully, the
   * internal buffered will be reset.
   *
   * @param out The output stream to write to.
   */
  public void writeTo(OutputStream out) throws IOException {
    output.writeTo(out);
    output.reset();
  }

  public int size() {
    return output.size();
  }

  /**
   * Writes raw bytes to the buffer without encoding. Same as calling
   *
   * {@link #writeRaw(byte[], int, int) writeRaw(rawBytes, 0, rawBytes.length)}.
   */
  public Encoder writeRaw(byte[] rawBytes) throws IOException {
    return writeRaw(rawBytes, 0, rawBytes.length);
  }

  /**
   * Writes raw bytes to the buffer without encoding.
   *
   * @param rawBytes The bytes to write.
   * @param off Offset to start in the byte array.
   * @param len Number of bytes to write starting from the offset.
   */
  public Encoder writeRaw(byte[] rawBytes, int off, int len) throws IOException {
    output.write(rawBytes, off, len);
    return this;
  }

  @Override
  public Encoder writeNull() throws IOException {
    encoder.writeNull();
    return this;
  }

  @Override
  public Encoder writeBool(boolean b) throws IOException {
    encoder.writeBool(b);
    return this;
  }

  @Override
  public Encoder writeInt(int i) throws IOException {
    encoder.writeInt(i);
    return this;
  }

  @Override
  public Encoder writeLong(long l) throws IOException {
    encoder.writeLong(l);
    return this;
  }

  @Override
  public Encoder writeFloat(float f) throws IOException {
    encoder.writeFloat(f);
    return this;
  }

  @Override
  public Encoder writeDouble(double d) throws IOException {
    encoder.writeDouble(d);
    return this;
  }

  @Override
  public Encoder writeString(String s) throws IOException {
    encoder.writeString(s);
    return this;
  }

  @Override
  public Encoder writeBytes(byte[] bytes) throws IOException {
    encoder.writeBytes(bytes);
    return this;
  }

  @Override
  public Encoder writeBytes(byte[] bytes, int off, int len) throws IOException {
    encoder.writeBytes(bytes, off, len);
    return this;
  }

  @Override
  public Encoder writeBytes(ByteBuffer bytes) throws IOException {
    encoder.writeBytes(bytes);
    return this;
  }
}
