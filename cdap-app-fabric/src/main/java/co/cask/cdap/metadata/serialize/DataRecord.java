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

package co.cask.cdap.metadata.serialize;

import java.util.Objects;

/**
 * Class to serialize {@link co.cask.cdap.proto.Id.DatasetInstance} and {@link co.cask.cdap.proto.Id.Stream}.
 */
public class DataRecord {
  private final String namespace;
  private final String type;
  private final String id;

  public DataRecord(String namespace, String type, String id) {
    this.namespace = namespace;
    this.type = type;
    this.id = id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DataRecord data = (DataRecord) o;
    return Objects.equals(namespace, data.namespace) &&
      Objects.equals(type, data.type) &&
      Objects.equals(id, data.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, type, id);
  }

  @Override
  public String toString() {
    return "DataRecord{" +
      "namespace='" + namespace + '\'' +
      ", type='" + type + '\'' +
      ", id='" + id + '\'' +
      '}';
  }
}
