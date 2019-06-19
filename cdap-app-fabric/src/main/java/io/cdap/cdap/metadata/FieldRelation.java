/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.metadata;

import java.util.Objects;

/**
 * This class represents the field relation about two dataset, source field -> destination field.
 */
public class FieldRelation {
  private final String source;
  private final String destination;

  public FieldRelation(String source, String destination) {
    this.source = source;
    this.destination = destination;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FieldRelation that = (FieldRelation) o;
    return Objects.equals(source, that.source) &&
      Objects.equals(destination, that.destination);
  }

  @Override
  public int hashCode() {
    return Objects.hash(source, destination);
  }

  public String getSource() {
    return source;
  }

  public String getDestination() {
    return destination;
  }
}
