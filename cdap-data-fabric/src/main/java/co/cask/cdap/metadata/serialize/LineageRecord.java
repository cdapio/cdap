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

package co.cask.cdap.metadata.serialize;

import co.cask.cdap.data2.metadata.lineage.Relation;
import co.cask.cdap.proto.Id;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.twill.api.RunId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Class to serialize {@link co.cask.cdap.data2.metadata.lineage.Lineage}.
 */
public class LineageRecord {
  private static final Function<RunId, String> RUN_ID_STRING_FUNCTION =
    new Function<RunId, String>() {
      @Override
      public String apply(RunId input) {
        return input.getId();
      }
    };
  private static final Function<Id.NamespacedId, String> ID_STRING_FUNCTION =
    new Function<Id.NamespacedId, String>() {
      @Nullable
      @Override
      public String apply(Id.NamespacedId input) {
        return input.getId();
      }
    };

  private final long start;
  private final long end;
  private final Set<RelationRecord> relations;
  private final Map<String, Map<String, ProgramRecord>> programs;
  private final Map<String, Map<String, DataRecord>> data;

  public LineageRecord(long start, long end, Set<Relation> lineageRelations) {
    this.start = start;
    this.end = end;
    this.relations = new HashSet<>();
    this.programs = new HashMap<>();
    this.data = new HashMap<>();

    addRelations(lineageRelations);
  }

  private void addRelations(Set<Relation> lineageRelations) {
    for (Relation relation : lineageRelations) {
      String dataKey = makeDataKey(relation.getData());
      String programKey = makeProgramKey(relation.getProgram());
      RelationRecord relationRecord = new RelationRecord(dataKey, programKey,
                                                      relation.getAccess().toString().toLowerCase(),
                                                      convertRuns(relation.getRun()),
                                                      convertComponents(relation.getComponents()));
      relations.add(relationRecord);
      programs.put(programKey, ImmutableMap.of("id", toProgramRecord(relation.getProgram())));
      data.put(dataKey, ImmutableMap.of("id", toDataRecord(relation.getData())));
    }
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

  private Set<String> convertRuns(RunId runId) {
    return ImmutableSet.of(runId.getId());
  }

  private Set<String> convertComponents(Set<Id.NamespacedId> components) {
    return Sets.newHashSet(Iterables.transform(components, ID_STRING_FUNCTION));
  }

  private String makeProgramKey(Id.Program program) {
    return Joiner.on('.').join(program.getType().getCategoryName().toLowerCase(), program.getNamespaceId(),
                               program.getApplicationId(), program.getId());
  }

  private ProgramRecord toProgramRecord(Id.Program program) {
    return new ProgramRecord(program.getNamespaceId(), program.getApplicationId(),
                            program.getType().getCategoryName().toLowerCase(), program.getId());
  }

  private String makeDataKey(Id.NamespacedId data) {
    if (data instanceof Id.DatasetInstance) {
      return makeDatasetKey((Id.DatasetInstance) data);
    }

    if (data instanceof  Id.Stream) {
      return makeStreamKey((Id.Stream) data);
    }

    throw new IllegalArgumentException("Unknown data object " + data);
  }

  private DataRecord toDataRecord(Id.NamespacedId data) {
    if (data instanceof Id.DatasetInstance) {
      return new DataRecord(((Id.DatasetInstance) data).getNamespaceId(), "dataset", data.getId());
    }

    if (data instanceof Id.Stream) {
      return new DataRecord(((Id.Stream) data).getNamespaceId(), "stream", data.getId());
    }

    throw new IllegalArgumentException("Unknown data object " + data);
  }

  private String makeDatasetKey(Id.DatasetInstance datasetInstance) {
    return Joiner.on('.').join("dataset", datasetInstance.getNamespaceId(), datasetInstance.getId());
  }

  private String makeStreamKey(Id.Stream stream) {
    return Joiner.on('.').join("stream", stream.getNamespaceId(), stream.getId());
  }
}
