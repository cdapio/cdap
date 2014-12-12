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
import co.cask.cdap.notifications.NotificationFeed;
import co.cask.common.io.ByteBufferInputStream;
import com.google.gson.Gson;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;

/**
 * Serializer/deserializer of Notifications to Kafka messages.
 */
public class KafkaMessageSerializer {
  private static final Gson GSON = new Gson();

  public static <N> byte[] encode(NotificationFeed feed, N notification) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    BinaryEncoder encoder = new BinaryEncoder(outputStream);

    String messageKey = buildKafkaMessageKey(feed);
    encoder.writeString(messageKey);
    // TODO should use something else than json to encode - avro or other
    encoder.writeString(GSON.toJson(notification));
    return outputStream.toByteArray();
  }

  public static <N> N decode(NotificationFeed feed, ByteBuffer byteBuffer, Type notificationType)
    throws IOException {
    BinaryDecoder decoder = new BinaryDecoder(new ByteBufferInputStream(byteBuffer));
    String msgKey = decoder.readString();
    if (!msgKey.equals(buildKafkaMessageKey(feed))) {
      return null;
    }
    return GSON.fromJson(decoder.readString(), notificationType);
  }

  public static String buildKafkaMessageKey(NotificationFeed feed) {
    return feed.getId();
  }
}
