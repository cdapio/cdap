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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.common.Bytes;

/**
 * Represents an incremental write to a datastore for in-memory buffering.  To read the current value, all
 * incremental writes for a field must be summed, along with the most recent {@link PutValue}, if any.
 */
public class IncrementValue implements Update<Long> {
  private final Long value;

  public IncrementValue(Long value) {
    this.value = value;
  }

  @Override
  public Long getValue() {
    return value;
  }

  @Override
  public byte[] getBytes() {
    return Bytes.toBytes(value);
  }

  @Override
  public Update<Long> deepCopy() {
    // it is immutable, safe to return itself
    return this;
  }
}
