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

import com.google.common.base.Objects;

import java.util.Iterator;
import java.util.Map;

/**
 * Contains metadata associated with a particular {@link Partition} of a {@link @PartitionedFileSet}
 */
public class PartitionMetadata implements Iterable<Map.Entry<String, String>> {
  private final Map<String, String> metadata;

  public PartitionMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }

  public String get(String key) {
    return metadata.get(key);
  }

  public Map<String, String> getAsMap() {
    return metadata;
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

    return Objects.equal(this.metadata, that.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(metadata);
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    return metadata.entrySet().iterator();
  }
}
