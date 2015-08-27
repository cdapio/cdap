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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.common.Bytes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Contains the state necessary to keep track of which partitions are processed and which partitions would need to be
 * processed as they are created.
 */
public class PartitionConsumerState {
  // useful on initial query of partitions
  public static final PartitionConsumerState FROM_BEGINNING =
    new PartitionConsumerState(0, Collections.<Long>emptyList());

  // Read pointer of the transaction from the previous query of partitions. This is used to scan for new partitions
  // created since then.
  private final long startVersion;
  // The list of in progress transactions from the previous query of partitions that are smaller than the startVersion.
  // We do not need to include the in-progress transaction Ids that are larger than the startVersion because those will
  // be picked up in the next scan anyways, since we will start the scan from the startVersion.
  // Keeping track of these in-progress transactions is necessary because these might be creations of partitions that
  // fall before the startVersion.
  private final List<Long> versionsToCheck;

  public PartitionConsumerState(long startVersion, List<Long> versionsToCheck) {
    if (versionsToCheck == null) {
      throw new IllegalArgumentException("List of versions cannot be null");
    }
    this.startVersion = startVersion;
    this.versionsToCheck = Collections.unmodifiableList(new ArrayList<>(versionsToCheck));
  }

  public long getStartVersion() {
    return startVersion;
  }

  public List<Long> getVersionsToCheck() {
    return versionsToCheck;
  }

  public static PartitionConsumerState fromBytes(byte[] bytes) {
    if (((bytes.length - 1) % Bytes.SIZEOF_LONG) != 0) {
      throw new IllegalArgumentException("bytes does not have length divisible by " + Bytes.SIZEOF_LONG);
    }

    ByteBuffer bb = ByteBuffer.wrap(bytes);
    byte serializationFormatVersion = bb.get();
    if (serializationFormatVersion != 0) {
      throw new IllegalArgumentException("Unsupported serialization format: " + serializationFormatVersion);
    }
    long startVersion = bb.getLong();
    List<Long> versionsToCheck = new ArrayList<>();
    while (bb.hasRemaining()) {
      versionsToCheck.add(bb.getLong());
    }
    return new PartitionConsumerState(startVersion, versionsToCheck);
  }

  public byte[] toBytes() {
    int numLongs = 1 + versionsToCheck.size();
    // first byte for serialization format version
    ByteBuffer bb = ByteBuffer.allocate(1 + Bytes.SIZEOF_LONG * numLongs);
    // currently, serialization format is 0
    bb.put((byte) 0);
    bb.putLong(startVersion);
    for (long l : versionsToCheck) {
      bb.putLong(l);
    }
    return bb.array();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PartitionConsumerState that = (PartitionConsumerState) o;

    if (startVersion != that.startVersion) {
      return false;
    }
    return versionsToCheck.equals(that.versionsToCheck);
  }

  @Override
  public int hashCode() {
    int result = (int) (startVersion ^ (startVersion >>> 32));
    result = 31 * result + versionsToCheck.hashCode();
    return result;
  }
}
