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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Contains metadata associated with a particular {@link Partition} of a {@link @PartitionedFileSet}
 */
public class PartitionMetadata implements Iterable<Map.Entry<String, String>> {
  private final Map<String, String> metadata;
  private final Long creationTime;

  public PartitionMetadata(Map<String, String> metadata, long creationTime) {
    this.metadata = Collections.unmodifiableMap(new HashMap<>(metadata));
    this.creationTime = creationTime;
  }

  /**
   * Perform a get on the underlying map of user-defined metadata.
   * @param key the key to use in the lookup on the underlying map.
   * @return the value returned by the underlying map.
   */
  @Nullable
  public String get(String key) {
    return metadata.get(key);
  }

  /**
   * @return the underlying map of user-defined metadata.
   */
  public Map<String, String> asMap() {
    return metadata;
  }

  /**
   * @return the creation time of the partition, in milliseconds.
   */
  public long getCreationTime() {
    return creationTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PartitionMetadata that = (PartitionMetadata) o;

    return this.metadata.equals(that.metadata) && this.creationTime.equals(that.creationTime);
  }

  @Override
  public int hashCode() {
    return metadata.hashCode() + 31 * creationTime.hashCode();
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    return metadata.entrySet().iterator();
  }
}
