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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A list of fields from a given stage.
 */
@Beta
public class JoinKey {
  private final String stageName;
  private final List<String> fields;

  public JoinKey(String stageName, List<String> fields) {
    this.stageName = stageName;
    this.fields = Collections.unmodifiableList(new ArrayList<>(fields));
  }

  public String getStageName() {
    return stageName;
  }

  public List<String> getFields() {
    return fields;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JoinKey that = (JoinKey) o;
    return Objects.equals(stageName, that.stageName) &&
      Objects.equals(fields, that.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(stageName, fields);
  }

  @Override
  public String toString() {
    return "JoinKey{" +
      "stageName='" + stageName + '\'' +
      ", fields=" + fields +
      '}';
  }
}
