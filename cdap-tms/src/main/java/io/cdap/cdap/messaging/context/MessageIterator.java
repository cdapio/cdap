/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.messaging.context;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.messaging.data.RawMessage;

/**
 * A {@link CloseableIterator} that converts each {@link RawMessage} to {@link Message}.
 */
final class MessageIterator extends AbstractCloseableIterator<Message> {

  private final CloseableIterator<RawMessage> rawIterator;

  MessageIterator(CloseableIterator<RawMessage> rawIterator) {
    this.rawIterator = rawIterator;
  }

  @Override
  protected Message computeNext() {
    if (!rawIterator.hasNext()) {
      return endOfData();
    }

    final RawMessage rawMessage = rawIterator.next();
    return new Message() {
      @Override
      public String getId() {
        return Bytes.toHexString(rawMessage.getId());
      }

      @Override
      public byte[] getPayload() {
        return rawMessage.getPayload();
      }

      @Override
      public String toString() {
        return "Message{" + "id=" + getId() + ",payload=" + getPayloadAsString() + "}";
      }
    };
  }

  @Override
  public void close() {
    rawIterator.close();
  }
}
