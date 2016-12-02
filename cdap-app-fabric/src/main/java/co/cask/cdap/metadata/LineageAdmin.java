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

package co.cask.cdap.metadata;

import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.entity.EntityExistenceVerifier;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.lineage.Lineage;
import co.cask.cdap.data2.metadata.lineage.LineageStore;
import co.cask.cdap.data2.metadata.lineage.LineageStoreReader;
import co.cask.cdap.data2.metadata.lineage.Relation;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Service to compute Lineage based on Dataset accesses of a Program stored in {@link LineageStore}.
 */
public class LineageAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(LineageAdmin.class);

  private static final Function<Relation, ProgramId> RELATION_TO_PROGRAM_FUNCTION =
    new Function<Relation, ProgramId>() {
      @Override
      public ProgramId apply(Relation input) {
        return input.getProgram();
      }
    };

  private static final Function<Relation, NamespacedEntityId> RELATION_TO_DATA_FUNCTION =
    new Function<Relation, NamespacedEntityId>() {
      @Override
      public NamespacedEntityId apply(Relation input) {
        return input.getData();
      }
    };

  private static final Predicate<Relation> UNKNOWN_TYPE_FILTER = new Predicate<Relation>() {
    @Override
    public boolean apply(Relation relation) {
      return relation.getAccess() != AccessType.UNKNOWN;
    }
  };

  private static final Function<Collection<Relation>, Collection<Relation>> COLLAPSE_UNKNOWN_TYPE_FUNCTION =
    new Function<Collection<Relation>, Collection<Relation>>() {
      @Override
      public Collection<Relation> apply(Collection<Relation> relations) {
        if (relations.size() <= 1) {
          return relations;
        }
        // If the size is > 1, then we can safely filter out the UNKNOWN
        return Collections2.filter(relations, UNKNOWN_TYPE_FILTER);
      }
    };

  private final LineageStoreReader lineageStoreReader;
  private final Store store;
  private final MetadataStore metadataStore;
  private final EntityExistenceVerifier entityExistenceVerifier;

  @Inject
  LineageAdmin(LineageStoreReader lineageStoreReader, Store store, MetadataStore metadataStore,
               EntityExistenceVerifier entityExistenceVerifier) {
    this.lineageStoreReader = lineageStoreReader;
    this.store = store;
    this.metadataStore = metadataStore;
    this.entityExistenceVerifier = entityExistenceVerifier;
  }

  /**
   * Computes lineage for a dataset between given time period.
   *
   * @param sourceDataset dataset to compute lineage for
   * @param startMillis start time period
   * @param endMillis end time period
   * @param levels number of levels to compute lineage for
   * @param rollup indicates whether to aggregate programs, currently supports rolling up programs into workflows
   * @return lineage for sourceDataset
   */
  public Lineage computeLineage(final DatasetId sourceDataset, long startMillis, long endMillis,
                                int levels, String rollup) throws NotFoundException {
    return doComputeLineage(sourceDataset, startMillis, endMillis, levels, rollup);
  }

  /**
   * Computes lineage for a dataset between given time period.
   *
   * @param sourceDataset dataset to compute lineage for
   * @param startMillis start time period
   * @param endMillis end time period
   * @param levels number of levels to compute lineage for
   * @return lineage for sourceDataset
   */
  public Lineage computeLineage(final DatasetId sourceDataset, long startMillis, long endMillis,
                                int levels) throws NotFoundException {
    return doComputeLineage(sourceDataset, startMillis, endMillis, levels, null);
  }

  /**
   * Computes lineage for a stream between given time period.
   *
   * @param sourceStream stream to compute lineage for
   * @param startMillis start time period
   * @param endMillis end time period
   * @param levels number of levels to compute lineage for
   * @param rollup lineage collapse rollup
   * @return lineage for sourceStream
   */
  public Lineage computeLineage(final StreamId sourceStream, long startMillis, long endMillis,
                                int levels, String rollup) throws NotFoundException {
    return doComputeLineage(sourceStream, startMillis, endMillis, levels, rollup);
  }

  /**
   * Computes lineage for a stream between given time period.
   *
   * @param sourceStream stream to compute lineage for
   * @param startMillis start time period
   * @param endMillis end time period
   * @param levels number of levels to compute lineage for
   * @return lineage for sourceStream
   */
  public Lineage computeLineage(final StreamId sourceStream, long startMillis, long endMillis,
                                int levels) throws NotFoundException {
    return doComputeLineage(sourceStream, startMillis, endMillis, levels, null);
  }

  /**
   * @return metadata associated with a run
   */
  public Set<MetadataRecord> getMetadataForRun(ProgramRunId run) throws NotFoundException {
    entityExistenceVerifier.ensureExists(run);

    Set<NamespacedEntityId> runEntities = new HashSet<>(lineageStoreReader.getEntitiesForRun(run));

    // No entities associated with the run, but run exists.
    if (runEntities.isEmpty()) {
      return ImmutableSet.of();
    }

    RunId runId = RunIds.fromString(run.getRun());

    // The entities returned by lineageStore does not contain application
    ApplicationId application = run.getParent().getParent();
    runEntities.add(application);
    return metadataStore.getSnapshotBeforeTime(MetadataScope.USER, runEntities,
                                               RunIds.getTime(runId, TimeUnit.MILLISECONDS));
  }

  @Nullable
  private ProgramRunId getWorkflowProgramRunid (Relation relation, Map<ProgramRunId,
    RunRecordMeta> runRecordMap, Map<String, ProgramRunId> workflowIdMap) {
    ProgramRunId workflowProgramRunId = null;

    RunRecordMeta runRecord = runRecordMap.get(
      new ProgramRunId(relation.getProgram().getNamespace(), relation.getProgram().getApplication(),
                       relation.getProgram().getType(), relation.getProgram().getProgram(),
                       relation.getRun().getId()));
    if (runRecord != null
      && runRecord.getProperties().containsKey("workflowrunid")) {
      String workflowRunId = runRecord.getProperties().get("workflowrunid");
      workflowProgramRunId = workflowIdMap.get(workflowRunId);
    }
    return workflowProgramRunId;
  }

  private Multimap<RelationKey, Relation> getRollupRelations (Multimap<RelationKey, Relation> relations,
                                                              Map<ProgramRunId, RunRecordMeta> runRecordMap,
                                                              Map<String, ProgramRunId> workflowIdMap)
    throws NotFoundException {

    Multimap<RelationKey, Relation> relationsNew = HashMultimap.create();
    for (Map.Entry<RelationKey, Collection<Relation>> entry : relations.asMap().entrySet()) {
      for (Relation relation : entry.getValue()) {
        ProgramRunId workflowProgramRunId = getWorkflowProgramRunid(relation, runRecordMap, workflowIdMap);
        if (workflowProgramRunId == null) {
          relationsNew.put(entry.getKey(), relation);
        } else {
          ProgramId workflowProgramId = new ProgramId(workflowProgramRunId.getNamespace(),
                                                        workflowProgramRunId.getApplication(),
                                                        workflowProgramRunId.getType(),
                                                        workflowProgramRunId.getProgram());
          Relation workflowRelation;
          NamespacedEntityId data = relation.getData();
          if (data instanceof DatasetId) {
            workflowRelation = new Relation((DatasetId) data, workflowProgramId,
                                              relation.getAccess(), RunIds.fromString(workflowProgramRunId.getRun()));
          } else {
            workflowRelation = new Relation((StreamId) data, workflowProgramId,
                                              relation.getAccess(), RunIds.fromString(workflowProgramRunId.getRun()));
          }
          relationsNew.put(entry.getKey(), workflowRelation);
        }
      }
    }
    return relationsNew;
  }

  private Set<String> getWorkflowIds (Multimap<RelationKey, Relation> relations,
                                      Map<ProgramRunId, RunRecordMeta> runRecordMap) throws NotFoundException {

    final Set<String> workflowIDs = new HashSet<>();
    for (Relation relation : Iterables.concat(relations.values())) {
      RunRecordMeta runRecord = runRecordMap.get(
        new ProgramRunId(relation.getProgram().getNamespace(), relation.getProgram().getApplication(),
                         relation.getProgram().getType(), relation.getProgram().getProgram(),
                         relation.getRun().getId()));
      if (runRecord != null && runRecord.getProperties().containsKey("workflowrunid")) {
        String workflowRunId = runRecord.getProperties().get("workflowrunid");
        workflowIDs.add(workflowRunId);
      }
    }

    return workflowIDs;
  }

  private Multimap<RelationKey, Relation> doComputeRollupLineage(Multimap<RelationKey,
    Relation> relations) throws NotFoundException {

    // Make a set of all ProgramIDs in the relations
    Set<ProgramRunId> programRunIdSet = new HashSet<>();
    for (Relation relation : Iterables.concat(relations.values())) {
      programRunIdSet.add(new ProgramRunId(relation.getProgram().getNamespace(), relation.getProgram().getApplication(),
                                        relation.getProgram().getType(), relation.getProgram().getProgram(),
                                        relation.getRun().getId()));
    }

    // Get RunRecordMeta for all these ProgramRunIDs
    final Map<ProgramRunId, RunRecordMeta> runRecordMap = store.getRuns(programRunIdSet);

    // Get workflow Run IDs for all the programs in the relations
    final Set<String> workflowIDs = getWorkflowIds(relations, runRecordMap);

    // Get Program IDs for workflow Run IDs
    // TODO: These scans could be expensive. CDAP-7571.
    Map<ProgramRunId, RunRecordMeta> workflowRunRecordMap =
      store.getRuns(ProgramRunStatus.ALL,
                    new Predicate<RunRecordMeta>() {
                      @Override
                      public boolean apply(RunRecordMeta input) {
                        return workflowIDs.contains(input.getPid());
                      }
                    });

    // Create a map from RunId to ProgramId for all workflows
    Map<String, ProgramRunId> workflowIdMap = new HashMap<>();
    for (Map.Entry<ProgramRunId, RunRecordMeta> entry : workflowRunRecordMap.entrySet()) {
      workflowIdMap.put(entry.getValue().getPid(), entry.getKey());
    }

    // For all relations, replace ProgramIds with workflow ProgramIds
    return getRollupRelations(relations, runRecordMap, workflowIdMap);
  }

  private Lineage doComputeLineage(final NamespacedEntityId sourceData, long startMillis, long endMillis,
                                   int levels, @Nullable String rollup) throws NotFoundException {
    LOG.trace("Computing lineage for data {}, startMillis {}, endMillis {}, levels {}",
              sourceData, startMillis, endMillis, levels);

    // Convert start time and end time period into scan keys in terms of program start times.
    Set<RunId> runningInRange = store.getRunningInRange(TimeUnit.MILLISECONDS.toSeconds(startMillis),
                                                        TimeUnit.MILLISECONDS.toSeconds(endMillis));
    if (LOG.isTraceEnabled()) {
      LOG.trace("Got {} rundIds in time range ({}, {})", runningInRange.size(), startMillis, endMillis);
    }

    ScanRangeWithFilter scanRange = getScanRange(runningInRange);
    LOG.trace("Using scan start = {}, scan end = {}", scanRange.getStart(), scanRange.getEnd());

    Multimap<RelationKey, Relation> relations = HashMultimap.create();
    Set<NamespacedEntityId> visitedDatasets = new HashSet<>();
    Set<NamespacedEntityId> toVisitDatasets = new HashSet<>();
    Set<ProgramId> visitedPrograms = new HashSet<>();
    Set<ProgramId> toVisitPrograms = new HashSet<>();

    toVisitDatasets.add(sourceData);
    for (int i = 0; i < levels; ++i) {
      LOG.trace("Level {}", i);
      toVisitPrograms.clear();
      for (NamespacedEntityId d : toVisitDatasets) {
        if (visitedDatasets.add(d)) {
          LOG.trace("Visiting dataset {}", d);
          // Fetch related programs
          Iterable<Relation> programRelations = getProgramRelations(d, scanRange.getStart(), scanRange.getEnd(),
                                                                    scanRange.getFilter());
          LOG.trace("Got program relations {}", programRelations);
          for (Relation relation : programRelations) {
            relations.put(new RelationKey(relation), relation);
          }
          Iterables.addAll(toVisitPrograms, Iterables.transform(programRelations, RELATION_TO_PROGRAM_FUNCTION));
        }
      }

      toVisitDatasets.clear();
      for (ProgramId p : toVisitPrograms) {
        if (visitedPrograms.add(p)) {
          LOG.trace("Visiting program {}", p);
          // Fetch related datasets
          Iterable<Relation> datasetRelations = lineageStoreReader.getRelations(p, scanRange.getStart(),
                                                                                scanRange.getEnd(),
                                                                                scanRange.getFilter());
          LOG.trace("Got data relations {}", datasetRelations);
          for (Relation relation : datasetRelations) {
            relations.put(new RelationKey(relation), relation);
          }
          Iterables.addAll(toVisitDatasets,
                           Iterables.transform(datasetRelations, RELATION_TO_DATA_FUNCTION));
        }
      }
    }

    if (rollup != null && rollup.contains("workflow")) {
      relations = doComputeRollupLineage(relations);
    }

    Lineage lineage = new Lineage(Iterables.concat(Maps.transformValues(relations.asMap(),
                                                                        COLLAPSE_UNKNOWN_TYPE_FUNCTION).values()));
    LOG.trace("Got lineage {}", lineage);
    return lineage;
  }

  private Iterable<Relation> getProgramRelations(NamespacedEntityId data, long start, long end,
                                                 Predicate<Relation> filter) {
    if (data instanceof DatasetId) {
      return lineageStoreReader.getRelations((DatasetId) data, start, end, filter);
    }

    if (data instanceof StreamId) {
      return lineageStoreReader.getRelations((StreamId) data, start, end, filter);
    }

    throw new IllegalStateException("Unknown data type " + data);
  }

  /**
   * Convert a set of runIds into a scan range based on earliest runtime and latest runtime of runIds.
   * Also, add a scan filter to include only runIds in the given set.
   * @param runIds input runIds set
   * @return scan range
   */
  @VisibleForTesting
  static ScanRangeWithFilter getScanRange(final Set<RunId> runIds) {
    if (runIds.isEmpty()) {
      return new ScanRangeWithFilter(0, 0, Predicates.<Relation>alwaysFalse());
    }

    // Pick the earliest start time and latest start time for lineage range
    long earliest = Long.MAX_VALUE;
    long latest = 0;
    for (RunId runId : runIds) {
      long runStartTime = RunIds.getTime(runId, TimeUnit.MILLISECONDS);
      if (runStartTime < earliest) {
        earliest = runStartTime;
      }
      if (runStartTime > latest) {
        latest = runStartTime;
      }
    }

    // scan end key is exclusive, so need to add 1 to  to include the last runid
    return new ScanRangeWithFilter(earliest, latest + 1, new Predicate<Relation>() {
      @Override
      public boolean apply(Relation input) {
        return runIds.contains(input.getRun());
      }
    });
  }

  @VisibleForTesting
  static class ScanRangeWithFilter {
    private final long start;
    private final long end;
    private final Predicate<Relation> filter;

    ScanRangeWithFilter(long start, long end, Predicate<Relation> filter) {
      this.start = start;
      this.end = end;
      this.filter = filter;
    }

    public long getStart() {
      return start;
    }

    public long getEnd() {
      return end;
    }

    public Predicate<Relation> getFilter() {
      return filter;
    }
  }

  /**
   * This class helps collapsing access type of {@link Relation} by ignoring the access type in equals and hashCode
   * so that it can be used as the map key for Relations of different access types.
   */
  private static final class RelationKey {
    private final Relation relation;
    private final int hashCode;

    private RelationKey(Relation relation) {
      this.relation = relation;
      this.hashCode = Objects.hash(relation.getData(), relation.getProgram(),
                                   relation.getRun(), relation.getComponents());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      // Don't use AccessType for equals (same for hashCode)
      RelationKey other = (RelationKey) o;
      return Objects.equals(relation.getData(), other.relation.getData()) &&
        Objects.equals(relation.getProgram(), other.relation.getProgram()) &&
        Objects.equals(relation.getRun(), other.relation.getRun()) &&
        Objects.equals(relation.getComponents(), other.relation.getComponents());
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }
}
