/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.proto.metadata.lineage;

import java.util.Objects;

/**
 * A class which contains field name and whether the field has lineage info present or not.
 */
public class Field {
  private final String name;
  private final boolean lineage;

  public Field(String name, boolean lineage) {
    this.name = name;
    this.lineage = lineage;
  }

  public String getName() {
    return name;
  }

  public boolean hasLineage() {
    return lineage;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Field field = (Field) o;
    return lineage == field.lineage &&
      Objects.equals(name, field.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, lineage);
  }

  @Override
  public String toString() {
    return "Field{" +
      "name='" + name + '\'' +
      ", lineage=" + lineage +
      '}';
  }
}
