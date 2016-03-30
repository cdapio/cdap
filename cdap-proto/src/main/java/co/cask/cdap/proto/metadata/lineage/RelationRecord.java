/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Class to serialize Relation.
 */
public class RelationRecord {
  private final String data;
  private final String program;
  private final Set<String> accesses;
  private final Set<String> runs;
  private final Set<String> components;

  public RelationRecord(String data, String program, Set<String> accesses, Set<String> runs, Set<String> components) {
    this.data = data;
    this.program = program;
    this.accesses = Collections.unmodifiableSet(new LinkedHashSet<>(accesses));
    this.runs = Collections.unmodifiableSet(new LinkedHashSet<>(runs));
    this.components = Collections.unmodifiableSet(new LinkedHashSet<>(components));
  }

  public String getData() {
    return data;
  }

  public String getProgram() {
    return program;
  }

  public Set<String> getAccesses() {
    return accesses;
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
    RelationRecord that = (RelationRecord) o;
    return Objects.equals(data, that.data) &&
      Objects.equals(program, that.program) &&
      Objects.equals(accesses, that.accesses) &&
      Objects.equals(runs, that.runs) &&
      Objects.equals(components, that.components);
  }

  @Override
  public int hashCode() {
    return Objects.hash(data, program, accesses, runs, components);
  }

  @Override
  public String toString() {
    return "RelationRecord{" +
      "data='" + data + '\'' +
      ", program='" + program + '\'' +
      ", accesses=" + accesses +
      ", runs=" + runs +
      ", components=" + components +
      '}';
  }
}
