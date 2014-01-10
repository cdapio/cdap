package com.continuuity.common.io;

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

  public void writeTo(OutputStream out) throws IOException {
    output.writeTo(out);
  }

  public int size() {
    return output.size();
  }

  @Override
  public Encoder writeNull() throws IOException {
    return encoder.writeNull();
  }

  @Override
  public Encoder writeBool(boolean b) throws IOException {
    return encoder.writeBool(b);
  }

  @Override
  public Encoder writeInt(int i) throws IOException {
    return encoder.writeInt(i);
  }

  @Override
  public Encoder writeLong(long l) throws IOException {
    return encoder.writeLong(l);
  }

  @Override
  public Encoder writeFloat(float f) throws IOException {
    return encoder.writeFloat(f);
  }

  @Override
  public Encoder writeDouble(double d) throws IOException {
    return encoder.writeDouble(d);
  }

  @Override
  public Encoder writeString(String s) throws IOException {
    return encoder.writeString(s);
  }

  @Override
  public Encoder writeBytes(byte[] bytes) throws IOException {
    return encoder.writeBytes(bytes);
  }

  @Override
  public Encoder writeBytes(byte[] bytes, int off, int len) throws IOException {
    return encoder.writeBytes(bytes, off, len);
  }

  @Override
  public Encoder writeBytes(ByteBuffer bytes) throws IOException {
    return encoder.writeBytes(bytes);
  }
}
