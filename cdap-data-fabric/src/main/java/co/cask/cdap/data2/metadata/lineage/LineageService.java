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
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Service to compute Lineage based on Dataset accesses of a Program stored in {@link LineageStore}.
 */
public class LineageService {
  private static final Logger LOG = LoggerFactory.getLogger(LineageService.class);

  private static final Function<Relation, Id.Program> RELATION_TO_PROGRAM_FUNCTION =
    new Function<Relation, Id.Program>() {
      @Override
      public Id.Program apply(Relation input) {
        return input.getProgram();
      }
    };

  private static final Function<Relation, Id.DatasetInstance> RELATION_TO_DATASET_INSTANCE_FUNCTION =
    new Function<Relation, Id.DatasetInstance>() {
      @Override
      public Id.DatasetInstance apply(Relation input) {
        return input.getData();
      }
    };

  private final LineageStore lineageStore;

  @Inject
  LineageService(LineageStore lineageStore) {
    this.lineageStore = lineageStore;
  }

  /**
   * Computes lineage for a dataset between given time period.
   *
   * @param sourceDataset dataset to compute lineage for
   * @param start start time period
   * @param end end time period
   * @param levels number of levels to compute lineage for
   * @return lineage for sourceDataset
   */
  public Lineage computeLineage(final Id.DatasetInstance sourceDataset, long start, long end, int levels) {
    LOG.trace("Computing lineage for dataset {}, start {}, end {}, levels {}", sourceDataset, start, end, levels);
    Set<Relation> relations = new HashSet<>();
    Set<Id.DatasetInstance> visitedDatasets = new HashSet<>();
    Set<Id.DatasetInstance> toVisitDatasets = new HashSet<>();
    Set<Id.Program> visitedPrograms = new HashSet<>();
    Set<Id.Program> toVisitPrograms = new HashSet<>();

    toVisitDatasets.add(sourceDataset);
    for (int i = 0; i < levels; ++i) {
      LOG.trace("Level {}", i);
      toVisitPrograms.clear();
      for (Id.DatasetInstance d : toVisitDatasets) {
        if (!visitedDatasets.contains(d)) {
          LOG.trace("Visiting dataset {}", d);
          visitedDatasets.add(d);
          // Fetch related programs
          Set<Relation> programRelations = lineageStore.getRelations(d, start, end);
          // Any read access of source Dataset can be pruned,
          // since those accessed do not contribute to lineage of source Dataset.
          // We are only interested relations that lead to write access to source Dataset.
          Iterable<Relation> prunedRelations =
            Iterables.filter(programRelations,
                             new Predicate<Relation>() {
                               @Override
                               public boolean apply(Relation relation) {
                                 return !(relation.getData().equals(sourceDataset) &&
                                   relation.getAccess() == AccessType.READ);
                               }
                             });
          LOG.trace("Got pruned program relations {}", prunedRelations);
          Iterables.addAll(relations, prunedRelations);
          Iterables.addAll(toVisitPrograms, Iterables.transform(prunedRelations, RELATION_TO_PROGRAM_FUNCTION));
        }
      }

      toVisitDatasets.clear();
      for (Id.Program p : toVisitPrograms) {
        if (!visitedPrograms.contains(p)) {
          LOG.trace("Visiting program {}", p);
          visitedPrograms.add(p);
          // Fetch related datasets
          Set<Relation> datasetRelations = lineageStore.getRelations(p, start, end);
          LOG.trace("Got dataset relations {}", datasetRelations);
          relations.addAll(datasetRelations);
          Iterables.addAll(toVisitDatasets,
                           Iterables.transform(datasetRelations, RELATION_TO_DATASET_INSTANCE_FUNCTION));
        }
      }
    }

    Lineage lineage = new Lineage(relations,
                                  Sets.union(visitedPrograms, toVisitPrograms),
                                  Sets.union(visitedDatasets, toVisitDatasets));
    LOG.trace("Got lineage {}", lineage);
    return lineage;
  }
}
