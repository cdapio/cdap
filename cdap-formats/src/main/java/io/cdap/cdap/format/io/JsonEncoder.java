/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.cdap.format.io;

import com.google.gson.stream.JsonWriter;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.common.io.Encoder;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

/**
 * An {@link Encoder} that encodes using {@link JsonWriter}.
 */
public final class JsonEncoder implements Encoder {

  private final JsonWriter jsonWriter;

  public JsonEncoder(JsonWriter jsonWriter) {
    this.jsonWriter = jsonWriter;
  }

  JsonWriter getJsonWriter() {
    return jsonWriter;
  }

  @Override
  public Encoder writeNull() throws IOException {
    jsonWriter.nullValue();
    return this;
  }

  @Override
  public Encoder writeBool(boolean b) throws IOException {
    jsonWriter.value(b);
    return this;
  }

  @Override
  public Encoder writeInt(int i) throws IOException {
    jsonWriter.value(i);
    return this;
  }

  @Override
  public Encoder writeLong(long l) throws IOException {
    jsonWriter.value(l);
    return this;
  }

  @Override
  public Encoder writeFloat(float f) throws IOException {
    jsonWriter.value(f);
    return this;
  }

  @Override
  public Encoder writeDouble(double d) throws IOException {
    jsonWriter.value(d);
    return this;
  }

  @Override
  public Encoder writeString(String s) throws IOException {
    jsonWriter.value(s);
    return this;
  }

  @Override
  public Encoder writeBytes(byte[] bytes) throws IOException {
    writeBytes(bytes, 0, bytes.length);
    return this;
  }

  @Override
  public Encoder writeBytes(byte[] bytes, int off, int len) throws IOException {
    jsonWriter.beginArray();
    for (int i = off; i < off + len; i++) {
      jsonWriter.value(bytes[i]);
    }
    jsonWriter.endArray();
    return this;
  }

  @Override
  public Encoder writeBytes(ByteBuffer bytes) throws IOException {
    if (bytes.hasArray()) {
      writeBytes(bytes.array(), bytes.arrayOffset() + bytes.position(), bytes.remaining());
    } else {
      byte[] buf = Bytes.getBytes(bytes);
      bytes.mark();
      bytes.get(buf);
      bytes.reset();
      writeBytes(buf, 0, buf.length);
    }
    return this;
  }

  @Override
  public Encoder writeNumber(Number number) throws IOException {
    // Wrap BigDecimal values so the output is always generated as the plain string value,
    if (number instanceof BigDecimal) {
      number = BigDecimalWrapper.wrap((BigDecimal) number);
    }

    jsonWriter.value(number);
    return this;
  }

  /**
   * Wrapper used to ensure that BigDecimals are generated as plain values (with no scientific
   * notation).
   */
  private static class BigDecimalWrapper extends Number {

    BigDecimal wrapped;

    protected static BigDecimalWrapper wrap(BigDecimal value) {
      return new BigDecimalWrapper(value);
    }

    protected BigDecimalWrapper(BigDecimal wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public String toString() {
      return wrapped.toPlainString();
    }

    @Override
    public int intValue() {
      return wrapped.intValue();
    }

    @Override
    public long longValue() {
      return wrapped.longValue();
    }

    @Override
    public float floatValue() {
      return wrapped.floatValue();
    }

    @Override
    public double doubleValue() {
      return wrapped.doubleValue();
    }
  }
}
