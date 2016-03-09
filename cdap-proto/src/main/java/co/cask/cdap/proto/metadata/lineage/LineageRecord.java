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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Class to serialize Lineage.
 */
public class LineageRecord {
  private final long start;
  private final long end;
  private final Set<RelationRecord> relations;
  private final Map<String, ProgramRecord> programs;
  private final Map<String, DataRecord> data;

  public LineageRecord(long start, long end, Set<RelationRecord> relations, Map<String, ProgramRecord> programs,
                       Map<String, DataRecord> data) {
    this.start = start;
    this.end = end;
    this.relations = ImmutableSet.copyOf(relations);
    this.programs = ImmutableMap.copyOf(programs);
    this.data = ImmutableMap.copyOf(data);
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public Set<RelationRecord> getRelations() {
    return relations;
  }

  public Map<String, ProgramRecord> getPrograms() {
    return programs;
  }

  public Map<String, DataRecord> getData() {
    return data;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LineageRecord that = (LineageRecord) o;
    return Objects.equals(start, that.start) &&
      Objects.equals(end, that.end) &&
      Objects.equals(relations, that.relations) &&
      Objects.equals(programs, that.programs) &&
      Objects.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(start, end, relations, programs, data);
  }

  @Override
  public String toString() {
    return "LineageRecord{" +
      "start=" + start +
      ", end=" + end +
      ", relations=" + relations +
      ", programs=" + programs +
      ", data=" + data +
      '}';
  }
}
