/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Contains metadata associated with a particular {@link Partition} of a {@link PartitionedFileSet}.
 */
public class PartitionMetadata implements Iterable<Map.Entry<String, String>> {
  private final Map<String, String> metadata;
  private final long creationTime;
  private final long lastModificationTime;

  public PartitionMetadata(Map<String, String> metadata, long creationTime, long lastModificationTime) {
    this.metadata = Collections.unmodifiableMap(new HashMap<>(metadata));
    this.creationTime = creationTime;
    this.lastModificationTime = lastModificationTime;
  }

  /**
   * Performs a get on the underlying map of user-defined metadata.
   *
   * @param key the key to use in the lookup on the underlying map
   * @return the value returned by the underlying map
   */
  @Nullable
  public String get(String key) {
    return metadata.get(key);
  }

  /**
   * @return the underlying map of user-defined metadata
   */
  public Map<String, String> asMap() {
    return metadata;
  }

  /**
   * @return the creation time of the partition, in milliseconds
   */
  public long getCreationTime() {
    return creationTime;
  }

  /**
   * @return the last modification time of the partition, in milliseconds
   */
  public long lastModificationTime() {
    return lastModificationTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PartitionMetadata other = (PartitionMetadata) o;
    return creationTime == other.creationTime
      && lastModificationTime == other.lastModificationTime
      && metadata.equals(other.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metadata, creationTime, lastModificationTime);
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    return metadata.entrySet().iterator();
  }
}
