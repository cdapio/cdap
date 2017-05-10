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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.io.Encoder;
import com.esotericsoftware.kryo.io.Output;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A {@link Encoder} for writing data to Kryo.
 */
public class KryoEncoder implements Encoder {

  private final Output output;

  public KryoEncoder(Output output) {
    this.output = output;
  }

  @Override
  public Encoder writeNull() throws IOException {
    return this;
  }

  @Override
  public Encoder writeBool(boolean b) throws IOException {
    output.writeBoolean(b);
    return this;
  }

  @Override
  public Encoder writeInt(int i) throws IOException {
    output.writeInt(i);
    return this;
  }

  @Override
  public Encoder writeLong(long l) throws IOException {
    output.writeLong(l);
    return this;
  }

  @Override
  public Encoder writeFloat(float f) throws IOException {
    output.writeFloat(f);
    return this;
  }

  @Override
  public Encoder writeDouble(double d) throws IOException {
    output.writeDouble(d);
    return this;
  }

  @Override
  public Encoder writeString(String s) throws IOException {
    output.writeString(s);
    return this;
  }

  @Override
  public Encoder writeBytes(byte[] bytes) throws IOException {
    writeBytes(bytes, 0, bytes.length);
    return this;
  }

  @Override
  public Encoder writeBytes(byte[] bytes, int off, int len) throws IOException {
    output.writeInt(len);
    output.writeBytes(bytes, off, len);
    return this;
  }

  @Override
  public Encoder writeBytes(ByteBuffer bytes) throws IOException {
    writeBytes(Bytes.getBytes(bytes));
    return this;
  }
}
