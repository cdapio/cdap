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

package co.cask.cdap.app.runtime.spark.serializer;

import co.cask.cdap.common.io.Decoder;
import com.esotericsoftware.kryo.io.Input;

import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;

/**
 * A {@link Decoder} for reading data from Kryo.
 */
public class KryoDecoder implements Decoder {

  private final Input input;

  public KryoDecoder(Input input) {
    this.input = input;
  }

  @Nullable
  @Override
  public Object readNull() throws IOException {
    return null;
  }

  @Override
  public boolean readBool() throws IOException {
    return input.readBoolean();
  }

  @Override
  public int readInt() throws IOException {
    return input.readInt();
  }

  @Override
  public long readLong() throws IOException {
    return input.readLong();
  }

  @Override
  public float readFloat() throws IOException {
    return input.readFloat();
  }

  @Override
  public double readDouble() throws IOException {
    return input.readDouble();
  }

  @Override
  public String readString() throws IOException {
    return input.readString();
  }

  @Override
  public ByteBuffer readBytes() throws IOException {
    int len = input.readInt();
    return ByteBuffer.wrap(input.readBytes(len));
  }

  @Override
  public void skipFloat() throws IOException {
    input.readFloat();
  }

  @Override
  public void skipDouble() throws IOException {
    input.readDouble();
  }

  @Override
  public void skipString() throws IOException {
    input.readString();
  }

  @Override
  public void skipBytes() throws IOException {
    input.skip(input.readInt());
  }
}
