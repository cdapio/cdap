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

package co.cask.cdap.internal.app.runtime.monitor;

import co.cask.cdap.api.common.Bytes;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;

/**
 * Message to monitor and collect
 */
public class MonitorMessage {
  private final String messageId;
  private final byte[] message;

  public MonitorMessage (GenericRecord record) {
    this.messageId = record.get("messageId").toString();
    this.message = Bytes.toBytes((ByteBuffer) record.get("message"));
  }

  public String getMessageId() {
    return messageId;
  }

  public byte[] getMessage() {
    return message;
  }
}
