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

package co.cask.cdap.data2.metadata.lineage;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.metadata.lineage.DataRecord;
import co.cask.cdap.proto.metadata.lineage.LineageRecord;
import co.cask.cdap.proto.metadata.lineage.ProgramRecord;
import co.cask.cdap.proto.metadata.lineage.RelationRecord;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.twill.api.RunId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Serializes {@link Lineage} into a {@link LineageRecord}.
 */
public final class LineageSerializer {
  private static final Function<Id.NamespacedId, String> ID_STRING_FUNCTION =
    new Function<Id.NamespacedId, String>() {
      @Nullable
      @Override
      public String apply(Id.NamespacedId input) {
        return input.getId();
      }
    };

  private LineageSerializer() {}

  public static LineageRecord toLineageRecord(long start, long end, Lineage lineage) {
    Set<RelationRecord> relationBuilder = new HashSet<>();
    Map<String, ProgramRecord> programBuilder = new HashMap<>();
    Map<String, DataRecord> dataBuilder = new HashMap<>();

    for (Relation relation : lineage.getRelations()) {
      String dataKey = makeDataKey(relation.getData());
      String programKey = makeProgramKey(relation.getProgram());
      RelationRecord relationRecord = new RelationRecord(dataKey, programKey,
                                                         relation.getAccess().toString().toLowerCase(),
                                                         convertRuns(relation.getRun()),
                                                         convertComponents(relation.getComponents()));
      relationBuilder.add(relationRecord);
      programBuilder.put(programKey, new ProgramRecord(relation.getProgram()));
      dataBuilder.put(dataKey, new DataRecord(relation.getData()));
    }
    return new LineageRecord(start, end, relationBuilder, programBuilder, dataBuilder);
  }

  private static Set<String> convertRuns(RunId runId) {
    return ImmutableSet.of(runId.getId());
  }

  private static Set<String> convertComponents(Set<Id.NamespacedId> components) {
    return Sets.newHashSet(Iterables.transform(components, ID_STRING_FUNCTION));
  }

  private static String makeProgramKey(Id.Program program) {
    return Joiner.on('.').join(program.getType().getCategoryName().toLowerCase(), program.getNamespaceId(),
                               program.getApplicationId(), program.getId());
  }

  private static String makeDataKey(Id.NamespacedId data) {
    if (data instanceof Id.DatasetInstance) {
      return makeDatasetKey((Id.DatasetInstance) data);
    }

    if (data instanceof  Id.Stream) {
      return makeStreamKey((Id.Stream) data);
    }

    throw new IllegalArgumentException("Unknown data object " + data);
  }

  private static String makeDatasetKey(Id.DatasetInstance datasetInstance) {
    return Joiner.on('.').join("dataset", datasetInstance.getNamespaceId(), datasetInstance.getId());
  }

  private static String makeStreamKey(Id.Stream stream) {
    return Joiner.on('.').join("stream", stream.getNamespaceId(), stream.getId());
  }
}
