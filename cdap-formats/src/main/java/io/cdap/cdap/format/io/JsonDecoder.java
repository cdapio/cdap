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

import co.cask.cdap.common.io.Decoder;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A {@link Decoder} that decodes from json using {@link JsonReader}.
 */
public class JsonDecoder implements Decoder {

  private final JsonReader jsonReader;

  public JsonDecoder(JsonReader jsonReader) {
    this.jsonReader = jsonReader;
  }

  JsonReader getJsonReader() {
    return jsonReader;
  }

  @Override
  public Object readNull() throws IOException {
    jsonReader.nextNull();
    return null;
  }

  @Override
  public boolean readBool() throws IOException {
    return jsonReader.nextBoolean();
  }

  @Override
  public int readInt() throws IOException {
    return jsonReader.nextInt();
  }

  @Override
  public long readLong() throws IOException {
    return jsonReader.nextLong();
  }

  @Override
  public float readFloat() throws IOException {
    return (float) jsonReader.nextDouble();
  }

  @Override
  public double readDouble() throws IOException {
    return jsonReader.nextDouble();
  }

  @Override
  public String readString() throws IOException {
    return jsonReader.nextString();
  }

  @Override
  public ByteBuffer readBytes() throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream(128);
    jsonReader.beginArray();
    while (jsonReader.peek() != JsonToken.END_ARRAY) {
      os.write(jsonReader.nextInt());
    }
    jsonReader.endArray();
    return ByteBuffer.wrap(os.toByteArray());
  }

  @Override
  public void skipFloat() throws IOException {
    readFloat();
  }

  @Override
  public void skipDouble() throws IOException {
    readDouble();
  }

  @Override
  public void skipString() throws IOException {
    readString();
  }

  @Override
  public void skipBytes() throws IOException {
    readBytes();
  }
}
