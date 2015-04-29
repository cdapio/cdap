/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.templates.etl.common.kafka;

import kafka.message.Message;
import org.apache.hadoop.fs.ChecksumException;

import java.io.IOException;

/**
 * Kafka Message.
 */
public class KafkaMessage implements co.cask.cdap.templates.etl.common.kafka.Message {

  byte[] payload;
  byte[] key;

  private String topic = "";
  private long offset = 0;
  private int partition = 0;
  private long checksum = 0;


  public KafkaMessage(byte[] payload, byte[] key, String topic, int partition, long offset, long checksum) {
    this.payload = payload;
    this.key = key;
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.checksum = checksum;
  }

  @Override
  public byte[] getPayload() {
    return payload;
  }

  @Override
  public byte[] getKey() {
    return key;
  }

  @Override
  public String getTopic() {
    return topic;
  }

  @Override
  public long getOffset() {
    return offset;
  }

  @Override
  public int getPartition() {
    return partition;
  }

  @Override
  public long getChecksum() {
    return checksum;
  }

  public void validate() throws IOException {
    // check the checksum of message.
    Message readMessage;
    if (key == null) {
      readMessage = new Message(payload);
    } else {
      readMessage = new Message(payload, key);
    }

    if (checksum != readMessage.checksum()) {
      throw new ChecksumException("Invalid message checksum : " + readMessage.checksum() + ". Expected " + checksum,
                                  offset);
    }
  }

}
