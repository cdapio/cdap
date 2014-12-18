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

package co.cask.cdap.notifications.kafka;

import co.cask.cdap.common.io.BinaryDecoder;
import co.cask.cdap.common.io.BinaryEncoder;
import co.cask.common.io.ByteBufferInputStream;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Serializer/deserializer of Notifications to Kafka messages.
 */
public class KafkaMessageIO {
  private static final Gson GSON = new Gson();

  public static ByteBuffer encode(KafkaMessage message) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    BinaryEncoder encoder = new BinaryEncoder(outputStream);

    // TODO should use something else than json to encode - avro or other
    encoder.writeString(GSON.toJson(message));
    return ByteBuffer.wrap(outputStream.toByteArray());
  }

  public static KafkaMessage decode(ByteBuffer byteBuffer)
    throws IOException {
    BinaryDecoder decoder = new BinaryDecoder(new ByteBufferInputStream(byteBuffer));
    try {
      return GSON.fromJson(decoder.readString(), KafkaMessage.class);
    } catch (JsonSyntaxException e) {
      return null;
    }
  }
}
