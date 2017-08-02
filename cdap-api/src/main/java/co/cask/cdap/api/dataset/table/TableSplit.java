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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Table splits are simply a start and stop key.
 */
public class TableSplit extends Split {

  private byte[] start, stop;

  /**
   * Constructor for serialization only. Don't call directly.
   */
  public TableSplit() {
    // No-op
  }

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
    return "TableSplit{" +
      "start=" + Bytes.toStringBinary(start) +
      ", stop=" + Bytes.toStringBinary(stop) +
      '}';
  }

  @Override
  public void writeExternal(DataOutput out) throws IOException {
    if (start == null) {
      out.writeInt(-1);
    } else {
      out.writeInt(start.length);
      out.write(start);
    }
    if (stop == null) {
      out.writeInt(-1);
    } else {
      out.writeInt(stop.length);
      out.write(stop);
    }
  }

  @Override
  public void readExternal(DataInput in) throws IOException {
    int len = in.readInt();
    if (len < 0) {
      start = null;
    } else {
      start = new byte[len];
      in.readFully(start);
    }

    len = in.readInt();
    if (len < 0) {
      stop = null;
    } else {
      stop = new byte[len];
      in.readFully(stop);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableSplit that = (TableSplit) o;
    return Arrays.equals(start, that.start) && Arrays.equals(stop, that.stop);
  }

  @Override
  public int hashCode() {
    return Objects.hash(start, stop);
  }
}
