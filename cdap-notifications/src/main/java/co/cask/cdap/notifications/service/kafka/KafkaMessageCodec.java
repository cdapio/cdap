/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.notifications.service.kafka;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Encoder/decoder of Notifications to Kafka messages.
 */
public class KafkaMessageCodec {
  private static final Gson GSON = new Gson();

  public static ByteBuffer encode(KafkaMessage message) throws IOException {
    return Charsets.UTF_8.encode(GSON.toJson(message));
  }

  public static KafkaMessage decode(ByteBuffer byteBuffer)
    throws IOException {
    try {
      return GSON.fromJson(Charsets.UTF_8.decode(byteBuffer).toString(), KafkaMessage.class);
    } catch (JsonSyntaxException e) {
      throw new IOException(e);
    }
  }
}
