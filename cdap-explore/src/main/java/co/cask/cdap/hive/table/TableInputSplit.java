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

package co.cask.cdap.hive.table;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Table;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Input split for a {@link Table} for Hive queries. Any InputSplit used by Hive MUST be a FileInputSplit otherwise
 * Hive will choke and die. Hive will pass this into HiveInputSplit, which happily returns an invalid path if this is
 * not a FileSplit. The invalid path causes mapred to die, but not in a way where any sensible error message is logged.
 */
public class TableInputSplit extends FileSplit {
  // name is needed here since it is not in the job conf by the time getRecordReader is called on the input format
  private String tableName;
  private byte[] start;
  private byte[] stop;

  // This is required for mapred
  public TableInputSplit() {
    this(null, null, null, null);
  }

  public TableInputSplit(String tableName, byte[] start, byte[] stop, Path dummyPath) {
    super(dummyPath, 0, 0, (String[]) null);
    this.tableName = tableName;
    this.start = start;
    this.stop = stop;
  }

  public String getTableName() {
    return tableName;
  }

  public byte[] getStartKey() {
    return start;
  }

  public byte[] getStopKey() {
    return stop;
  }

  @Override
  public String[] getLocations() throws IOException {
    return new String[0];
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    super.write(out);
    writeBytes(out, Bytes.toBytes(tableName));
    writeBytes(out, start);
    writeBytes(out, stop);
  }

  private void writeBytes(DataOutput out, byte[] bytes) throws IOException {
    if (bytes == null) {
      out.writeInt(0);
    } else {
      out.writeInt(bytes.length);
      out.write(bytes);
    }
  }

  @Override
  public void readFields(final DataInput in) throws IOException {
    super.readFields(in);
    byte[] nameBytes = readBytes(in);
    tableName = Bytes.toString(nameBytes);
    start = readBytes(in);
    stop = readBytes(in);
  }

  private byte[] readBytes(DataInput in) throws IOException {
    int length = in.readInt();
    if (length == 0) {
      return null;
    }
    byte[] bytes = new byte[length];
    in.readFully(bytes);
    return bytes;
  }
}
