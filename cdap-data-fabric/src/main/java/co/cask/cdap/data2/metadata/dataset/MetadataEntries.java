/*
 * Copyright © 2018 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.cdap.data2.metadata.dataset;

import co.cask.cdap.api.dataset.lib.KeyValue;

import java.util.List;
import java.util.Objects;

/**
 * Holds information needed for metadata migration
 */
public class MetadataEntries {
  private final List<KeyValue<Long, MetadataEntry>> entries;
  private final List<byte[]> rows;

  public MetadataEntries(List<KeyValue<Long, MetadataEntry>> entries, List<byte[]> rows) {
    this.entries = entries;
    this.rows = rows;
  }

  public List<KeyValue<Long, MetadataEntry>> getEntries() {
    return entries;
  }

  public List<byte[]> getRows() {
    return rows;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MetadataEntries that = (MetadataEntries) o;

    return Objects.equals(entries, that.entries) && Objects.equals(rows, that.rows);
  }

  @Override
  public int hashCode() {
    return Objects.hash(entries, rows);
  }

  @Override
  public String toString() {
    return "MetadataEntries{" +
      "entries=" + entries +
      ", rows=" + rows +
      '}';
  }
}
