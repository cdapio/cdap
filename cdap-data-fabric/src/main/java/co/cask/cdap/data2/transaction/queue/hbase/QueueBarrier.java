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

package co.cask.cdap.data2.transaction.queue.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.data2.queue.ConsumerGroupConfig;
import com.google.common.base.Objects;

/**
 * Representing queue barrier information. It contains the consumer group information for
 * queue entry that are enqueue after the given start row.
 */
public final class QueueBarrier {
  private final ConsumerGroupConfig groupConfig;
  private final byte[] startRow;

  QueueBarrier(ConsumerGroupConfig groupConfig, byte[] startRow) {
    this.groupConfig = groupConfig;
    this.startRow = startRow;
  }

  public ConsumerGroupConfig getGroupConfig() {
    return groupConfig;
  }

  public byte[] getStartRow() {
    return startRow;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("group", groupConfig)
      .add("start", Bytes.toStringBinary(startRow))
      .toString();
  }
}
