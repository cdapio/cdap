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
package co.cask.cdap.data2.transaction.queue.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.data2.queue.ConsumerConfig;
import com.google.common.base.Objects;

import javax.annotation.Nullable;

/**
 * Represents state of a queue consumer.
 */
public final class HBaseConsumerState {

  private final ConsumerConfig consumerConfig;
  private final byte[] startRow;
  private final byte[] barrierStartRow;
  private final byte[] barrierEndRow;

  HBaseConsumerState(ConsumerConfig consumerConfig, byte[] startRow,
                     @Nullable byte[] barrierStartRow, @Nullable byte[] barrierEndRow) {
    this.consumerConfig = consumerConfig;
    this.startRow = startRow;
    this.barrierStartRow = barrierStartRow;
    this.barrierEndRow = barrierEndRow;
  }

  public ConsumerConfig getConsumerConfig() {
    return consumerConfig;
  }

  public byte[] getStartRow() {
    return startRow;
  }

  @Nullable
  public byte[] getBarrierStartRow() {
    return barrierStartRow;
  }

  @Nullable
  public byte[] getBarrierEndRow() {
    return barrierEndRow;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("config", consumerConfig)
      .add("start", Bytes.toStringBinary(startRow))
      .add("barrierStart", Bytes.toStringBinary(barrierStartRow))
      .add("barrierEnd", Bytes.toStringBinary(barrierEndRow))
      .toString();
  }
}
