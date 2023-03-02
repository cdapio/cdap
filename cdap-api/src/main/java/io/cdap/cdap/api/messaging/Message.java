/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package io.cdap.cdap.api.messaging;

import io.cdap.cdap.api.annotation.Beta;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/**
 * Represents a message in the Transactional Messaging System.
 */
@Beta
public interface Message {

  /**
   * Returns the unique identifier of this message.
   */
  String getId();

  /**
   * Returns the message payload as a string decoded with the given {@link Charset}.
   *
   * @param charset the {@link Charset} to use for decoding
   * @return the message payload as a string
   */
  default String getPayloadAsString(Charset charset) {
    return new String(getPayload(), charset);
  }

  /**
   * Returns the message payload as a UTF-8 string.
   *
   * @return a UTF-8 string representation of the message payload
   */
  default String getPayloadAsString() {
    return getPayloadAsString(StandardCharsets.UTF_8);
  }

  /**
   * Decodes the message payload.
   *
   * @param decoder a {@link Function} to decode the object from a provided {@link Reader} that
   *     reads the payload as {@link StandardCharsets#UTF_8} string.
   * @param <T> the instance type of the decoded object
   * @return the decoded object
   */
  default <T> T decodePayload(Function<Reader, T> decoder) {
    try (Reader reader = new InputStreamReader(new ByteArrayInputStream(getPayload()),
        StandardCharsets.UTF_8)) {
      return decoder.apply(reader);
    } catch (IOException e) {
      // This should never happen as we are reading from byte[]
      throw new RuntimeException("Failed to deserialize message payload", e);
    }
  }

  /**
   * Returns the message payload.
   */
  byte[] getPayload();
}
