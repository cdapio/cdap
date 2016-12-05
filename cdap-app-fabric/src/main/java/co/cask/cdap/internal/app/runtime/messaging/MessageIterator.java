/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.messaging;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.AbstractCloseableIterator;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.messaging.data.RawMessage;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

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
      public String getPayloadAsString(Charset charset) {
        return new String(getPayload(), charset);
      }

      @Override
      public String getPayloadAsString() {
        return getPayloadAsString(StandardCharsets.UTF_8);
      }

      @Override
      public byte[] getPayload() {
        return rawMessage.getPayload();
      }
    };
  }

  @Override
  public void close() {
    rawIterator.close();
  }
}
