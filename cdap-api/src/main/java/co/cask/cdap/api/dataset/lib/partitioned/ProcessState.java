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

package co.cask.cdap.api.dataset.lib.partitioned;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents the process/consumed state of the partition. Partitions that have been successfully processed
 * are no longer held reference to. The ConsumablePartition corresponding to that partition will be removed from
 * the working set at that time
 *
 * AVAILABLE - available for processing
 * IN_PROGRESS - currently being processed
 */
public enum ProcessState {
  AVAILABLE(0), IN_PROGRESS(1), DISCARDED(2), COMPLETED(3);

  private final byte bits;

  ProcessState(int i) {
    this.bits = (byte) i;
  }

  // helper map for efficient implementation of scopeFor()
  private static final Map<Byte, ProcessState> LOOKUP_BY_BYTE;
  static {
    LOOKUP_BY_BYTE = new HashMap<>();
    for (ProcessState state: values()) {
      LOOKUP_BY_BYTE.put(state.toByte(), state);
    }
  }

  public static ProcessState fromByte(byte bits) {
    ProcessState state = LOOKUP_BY_BYTE.get(bits);
    if (state != null) {
      return state;
    }
    throw new IllegalArgumentException("Unknown numeric code for process state: " + bits);
  }

  public byte toByte() {
    return bits;
  }
}
