/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

/**
 * Represents a column inside a query result.
 */
public class ColumnDesc {
  private final String name;
  private final String type;
  private final int position;
  private final String comment;

  public ColumnDesc(String name, String type, int position, String comment) {
    this.name = name;
    this.type = type;
    this.position = position;
    this.comment = comment;
  }

  @SuppressWarnings("UnusedDeclaration")
  public String getName() {
    return name;
  }

  @SuppressWarnings("UnusedDeclaration")
  public String getType() {
    return type;
  }

  @SuppressWarnings("UnusedDeclaration")
  public int getPosition() {
    return position;
  }

  @SuppressWarnings("UnusedDeclaration")
  public String getComment() {
    return comment;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ColumnDesc that = (ColumnDesc) o;

    return Objects.equals(this.name, that.name) &&
      Objects.equals(this.type, that.type) &&
      Objects.equals(this.position, that.position) &&
      Objects.equals(this.comment, that.comment);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, position, comment);
  }

  @Override
  public String toString() {
    return "ColumnDesc{" +
      "name='" + name + '\'' +
      ", type='" + type + '\'' +
      ", position=" + position +
      ", comment='" + comment + '\'' +
      '}';
  }
}
