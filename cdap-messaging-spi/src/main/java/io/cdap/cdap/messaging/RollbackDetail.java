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

package io.cdap.cdap.messaging;

/**
 * This interface represents information needed to rollback message published transactionally.
 */
public interface RollbackDetail {

  /**
   * Returns the transaction write pointer used when the message was published.
   */
  long getTransactionWritePointer();

  /**
   * Returns the timestamp being used for the first payload published with the given transaction write pointer.
   */
  long getStartTimestamp();

  /**
   * Returns the sequence id being used for the first payload published with the given transaction write pointer.
   */
  int getStartSequenceId();

  /**
   * Returns the timestamp being used for the last payload published with the given transaction write pointer.
   */
  long getEndTimestamp();

  /**
   * Returns the sequence id being used for the last payload published with the given transaction write pointer.
   */
  int getEndSequenceId();
}
