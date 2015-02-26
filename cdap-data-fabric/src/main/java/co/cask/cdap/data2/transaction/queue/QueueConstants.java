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
package co.cask.cdap.data2.transaction.queue;

/**
 * Constants for queue implementation in HBase.
 */
public final class QueueConstants {

  /**
   * Configuration keys for queues in HBase.
   */
  public static final class ConfigKeys {
    public static final String QUEUE_TABLE_COPROCESSOR_DIR = "data.queue.table.coprocessor.dir";
    public static final String QUEUE_TABLE_PRESPLITS = "data.queue.table.presplits";
    public static final String DEQUEUE_TX_PERCENT = "data.queue.dequeue.tx.percent";
  }

  public static final String QUEUE_CONFIG_TABLE_NAME = QueueType.QUEUE.toString() + ".config";

  public static final String DEFAULT_QUEUE_TABLE_COPROCESSOR_DIR = "/queue";
  public static final int DEFAULT_QUEUE_TABLE_PRESPLITS = 16;

  public static final long MAX_CREATE_TABLE_WAIT = 5000L;    // Maximum wait of 5 seconds for table creation.

  // How frequently (in seconds) to update the ConsumerConfigCache data for the HBaseQueueRegionObserver
  public static final String QUEUE_CONFIG_UPDATE_FREQUENCY = "data.queue.config.update.interval";
  public static final Long DEFAULT_QUEUE_CONFIG_UPDATE_FREQUENCY = 5L; // default to 5 seconds

  /**
   * whether a queue is a queue or a stream.
   */
  public enum QueueType {

    QUEUE("queue"),
    STREAM("stream");

    private final String string;

    QueueType(String string) {
      this.string = string;
    }

    @Override
    public String toString() {
      return string;
    }
  }

  private QueueConstants() {
  }
}
