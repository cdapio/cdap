/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.join;

import io.cdap.cdap.api.annotation.Beta;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * The name of a field and an optional alias to rename it to.
 */
@Beta
public class JoinField {
  private final String stageName;
  private final String fieldName;
  private final String alias;

  public JoinField(String stageName, String fieldName) {
    this(stageName, fieldName, null);
  }

  public JoinField(String stageName, String fieldName, @Nullable String alias) {
    this.stageName = stageName;
    this.fieldName = fieldName;
    this.alias = alias;
  }

  public String getStageName() {
    return stageName;
  }

  public String getFieldName() {
    return fieldName;
  }

  @Nullable
  public String getAlias() {
    return alias;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JoinField field1 = (JoinField) o;
    return Objects.equals(stageName, field1.stageName) &&
      Objects.equals(fieldName, field1.fieldName) &&
      Objects.equals(alias, field1.alias);
  }

  @Override
  public int hashCode() {
    return Objects.hash(stageName, fieldName, alias);
  }

  @Override
  public String toString() {
    return "Field{" +
      "stage='" + stageName + '\'' +
      ", field='" + fieldName + '\'' +
      ", alias='" + alias + '\'' +
      '}';
  }
}
