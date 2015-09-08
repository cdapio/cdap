/*
 * Copyright 2015 Cask Data, Inc.
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
package co.cask.cdap.proto;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represent Busineess Metadata for CDAP.
 */
public class BusinessMetadataRecord {
  private final String targetType;
  private final Id.NamespacedId  targetId;
  private final String key;
  private final String value;
  private final String schema;

  public BusinessMetadataRecord(String targetType, Id.NamespacedId targetId, String key, String value) {
    this(targetType, targetId, key, value, null);
  }

  public BusinessMetadataRecord(String targetType, Id.NamespacedId  targetId, String key, String value,
                                @Nullable String schema) {
    this.targetType = targetType;
    this.targetId = targetId;
    this.key = key;
    this.value = value;
    this.schema = schema;
  }

  public String getTargetType() {
    return targetType;
  }

  public Id.NamespacedId  getTargetId() {
    return targetId;
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  public String getSchema() {
    return schema;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BusinessMetadataRecord that = (BusinessMetadataRecord) o;

    return Objects.equals(targetType, that.targetType) &&
      Objects.equals(targetId, that.targetId) &&
      Objects.equals(key, that.key) &&
      Objects.equals(value, that.value) &&
      Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(targetType, targetId, key, value, schema);
  }

  @Override
  public String toString() {
    return "{" +
      "targetType='" + targetType + '\'' +
      ", targetId='" + targetId + '\'' +
      ", key='" + key + '\'' +
      ", value='" + value + '\'' +
      ", schema='" + (schema != null ? schema : 0) +
      '}';
  }
}
