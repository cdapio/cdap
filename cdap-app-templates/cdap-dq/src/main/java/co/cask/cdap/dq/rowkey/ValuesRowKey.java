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

package co.cask.cdap.dq.rowkey;

import co.cask.cdap.api.common.Bytes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Class for constructing tableRowKeys that consist of the byte[] representation of "Values"
 * concatenated with sourcdID, fieldName, and then timestamp.
 */
public class ValuesRowKey {
  private byte[] tableRowKey;
  private final byte[] prefixBytes = Bytes.toBytes("v"); // stands for "values"

  public ValuesRowKey(long timestamp, String fieldName, String sourceID) throws IOException {
    byte[] timestampBytes = Bytes.toBytes(timestamp);
    byte[] fieldNameBytes = Bytes.toBytes(fieldName);
    byte[] sourceIDBytes = Bytes.toBytes(sourceID);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    outputStream.write(prefixBytes);
    outputStream.write(sourceIDBytes);
    outputStream.write(fieldNameBytes);
    outputStream.write(timestampBytes);

    tableRowKey = outputStream.toByteArray();
  }

  public byte[] getTableRowKey() {
    return tableRowKey;
  }
}
