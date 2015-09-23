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

package co.cask.cdap.proto.metadata.lineage;

import com.google.common.collect.ImmutableSet;

import java.util.Objects;
import java.util.Set;

/**
 * Class to serialize Relation.
 */
public class RelationRecord {
  private final String data;
  private final String program;
  private final String access;
  private final Set<String> runs;
  private final Set<String> components;

  public RelationRecord(String data, String program, String access, Set<String> runs, Set<String> components) {
    this.data = data;
    this.program = program;
    this.access = access;
    this.runs = ImmutableSet.copyOf(runs);
    this.components = ImmutableSet.copyOf(components);
  }

  public String getData() {
    return data;
  }

  public String getProgram() {
    return program;
  }

  public String getAccess() {
    return access;
  }

  public Set<String> getRuns() {
    return runs;
  }

  public Set<String> getComponents() {
    return components;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RelationRecord relation = (RelationRecord) o;
    return Objects.equals(data, relation.data) &&
      Objects.equals(program, relation.program) &&
      Objects.equals(access, relation.access) &&
      Objects.equals(runs, relation.runs) &&
      Objects.equals(components, relation.components);
  }

  @Override
  public int hashCode() {
    return Objects.hash(data, program, access, runs, components);
  }

  @Override
  public String toString() {
    return "RelationRecord{" +
      "data='" + data + '\'' +
      ", program='" + program + '\'' +
      ", access='" + access + '\'' +
      ", runs=" + runs +
      ", components=" + components +
      '}';
  }
}
