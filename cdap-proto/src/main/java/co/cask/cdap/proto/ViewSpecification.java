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
package co.cask.cdap.proto;

import co.cask.cdap.api.data.format.FormatSpecification;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents the configuration of a stream view, used when creating or updating a view.
 */
public class ViewSpecification {

  private final FormatSpecification format;

  /**
   * The Hive table name to use. If null in the creation or update request,
   * use default name generated from {@link Id.Stream.View} before persisting this.
   */
  private final String tableName;

  public ViewSpecification(FormatSpecification format, String tableName) {
    this.format = format;
    this.tableName = tableName;
  }

  public ViewSpecification(FormatSpecification format) {
    this(format, null);
  }

  public ViewSpecification(ViewSpecification other) {
    this(other.format, other.tableName);
  }

  public FormatSpecification getFormat() {
    return format;
  }

  @Nullable
  public String getTableName() {
    return tableName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ViewSpecification that = (ViewSpecification) o;
    return Objects.equals(format, that.format) && Objects.equals(tableName, that.tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(format, tableName);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ViewSpecification{");
    sb.append("format=").append(format);
    sb.append(", tableName='").append(tableName).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
