/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.api.dataset.table;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Split;
import com.google.common.base.Objects;

/**
 * Table splits are simply a start and stop key.
 */
public class TableSplit extends Split {
  private final byte[] start, stop;

  public TableSplit(byte[] start, byte[] stop) {
    this.start = start;
    this.stop = stop;
  }

  public byte[] getStart() {
    return start;
  }

  public byte[] getStop() {
    return stop;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("start", Bytes.toString(start))
      .add("stop", Bytes.toString(stop))
      .toString();
  }
}
