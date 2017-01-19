/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.replication;

import co.cask.cdap.api.common.Bytes;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Handling of Row keys for the Replication State Table
 */
public final class ReplicationStatusKey {

  private final String rowType;
  private final String regionName;
  private final UUID rsID;
  private final byte[] key;

  public ReplicationStatusKey(String rowType, String regionName, UUID regionServerID) {
    byte[] rowKey;
    rowKey = Bytes.add(Bytes.toBytes(Bytes.toBytes(rowType).length), Bytes.toBytes(rowType));
    rowKey = Bytes.add(rowKey, Bytes.toBytes(Bytes.toBytes(regionName).length), Bytes.toBytes(regionName));
    rowKey = Bytes.add(rowKey, Bytes.toBytes(regionServerID));
    this.rowType = rowType;
    this.regionName = regionName;
    this.rsID = regionServerID;
    this.key = rowKey;
  }

  public ReplicationStatusKey(byte[] key) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(key);
    this.key = key;
    this.rowType = Bytes.toString(getBytes(byteBuffer));
    this.regionName = Bytes.toString(getBytes(byteBuffer));
    this.rsID = new UUID(byteBuffer.getLong(), byteBuffer.getLong());
  }

  /**
   * @return the underlying key
   */
  public byte[] getKey() {
    return key;
  }

  public String getRowType() {
    return rowType;
  }

  public String getRegionName() {
    return regionName;
  }

  public UUID getRsID() {
    return rsID;
  }

  /**
   * @return the next byte[] part in the splitter
   * @throws BufferUnderflowException if there is no byte[] as expected
   */
  private byte[] getBytes(ByteBuffer byteBuffer) {
    int len = byteBuffer.getInt();
    if (byteBuffer.remaining() < len) {
      throw new BufferUnderflowException();
    }
    byte[] bytes = new byte[len];
    byteBuffer.get(bytes, 0, len);
    return bytes;
  }
}
