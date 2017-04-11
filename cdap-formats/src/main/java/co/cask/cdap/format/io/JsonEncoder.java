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

package co.cask.cdap.format.io;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.io.Encoder;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
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
}
