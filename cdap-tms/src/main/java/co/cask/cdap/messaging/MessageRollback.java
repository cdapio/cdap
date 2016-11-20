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

package co.cask.cdap.messaging;

/**
 * This class contains information needed for performing rollback operation transactional publish request.
 */
public final class MessageRollback {

  private final long transactionWritePointer;
  private final long startTimestamp;
  private final int startSequenceId;
  private final long endTimestamp;
  private final int endSequenceId;

  public MessageRollback(long transactionWritePointer, long startTimestamp,
                         int startSequenceId, long endTimestamp, int endSequenceId) {
    this.transactionWritePointer = transactionWritePointer;
    this.startTimestamp = startTimestamp;
    this.startSequenceId = startSequenceId;
    this.endTimestamp = endTimestamp;
    this.endSequenceId = endSequenceId;
  }

  public long getTransactionWritePointer() {
    return transactionWritePointer;
  }

  public long getStartTimestamp() {
    return startTimestamp;
  }

  public int getStartSequenceId() {
    return startSequenceId;
  }

  public long getEndTimestamp() {
    return endTimestamp;
  }

  public int getEndSequenceId() {
    return endSequenceId;
  }
}
