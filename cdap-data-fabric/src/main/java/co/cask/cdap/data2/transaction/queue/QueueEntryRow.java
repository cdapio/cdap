/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

import co.cask.cdap.api.common.Bytes;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

/**
 * Holds logic of how queue entry row is constructed
 */
public class QueueEntryRow {
  public static final byte[] COLUMN_FAMILY = new byte[] {'q'};
  public static final byte[] STATE_COLUMN_PREFIX = new byte[] {'s'};

  /**
   * @param stateValue value of the state column
   * @return write pointer of the latest change of the state value
   */
  public static long getStateWritePointer(byte[] stateValue) {
    return Bytes.toLong(stateValue, 0, Longs.BYTES);
  }

  /**
   * @param stateValue value of the state column
   * @return state instance id
   */
  public static int getStateInstanceId(byte[] stateValue) {
    return Bytes.toInt(stateValue, Longs.BYTES, Ints.BYTES);
  }

  /**
   * @param stateValue value of the state column
   * @return consumer entry state
   */
  public static ConsumerEntryState getState(byte[] stateValue) {
    return ConsumerEntryState.fromState(stateValue[Longs.BYTES + Ints.BYTES]);
  }

  // Consuming logic

  /**
   * Gets the write pointer for a row.
   */
  public static long getWritePointer(byte[] rowKey, int queueRowPrefixLength) {
    // Row key is queue_name + writePointer + counter
    return Bytes.toLong(rowKey, queueRowPrefixLength, Longs.BYTES);
  }
}
