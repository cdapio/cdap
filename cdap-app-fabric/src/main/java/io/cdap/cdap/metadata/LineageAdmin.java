/*
 * Copyright © 2015-2020 Cask Data, Inc.
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.workflow.WorkflowActionNode;
import io.cdap.cdap.api.workflow.WorkflowNode;
import io.cdap.cdap.api.workflow.WorkflowSpecification;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.data2.metadata.lineage.AccessType;
import io.cdap.cdap.data2.metadata.lineage.DefaultLineageStoreReader;
import io.cdap.cdap.data2.metadata.lineage.Lineage;
import io.cdap.cdap.data2.metadata.lineage.LineageStoreReader;
import io.cdap.cdap.data2.metadata.lineage.Relation;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.WorkflowId;
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
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Service to compute Lineage based on Dataset accesses of a Program stored in {@link DefaultLineageStoreReader}.
 */
public class LineageAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(LineageAdmin.class);

  private static final Function<Collection<Relation>, Collection<Relation>> COLLAPSE_UNKNOWN_TYPE_FUNCTION =
    relations -> {
      if (relations.size() <= 1) {
        return relations;
      }
      // If the size is > 1, then we can safely filter out the UNKNOWN
      return Collections2.filter(relations, relation -> relation.getAccess() != AccessType.UNKNOWN);
    };

  private final LineageStoreReader lineageStoreReader;
  private final Store store;

  @Inject
  LineageAdmin(LineageStoreReader lineageStoreReader, Store store) {
    this.lineageStoreReader = lineageStoreReader;
    this.store = store;
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
  public Lineage computeLineage(DatasetId sourceDataset, long startMillis, long endMillis,
                                int levels, String rollup) {
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
  public Lineage computeLineage(DatasetId sourceDataset, long startMillis, long endMillis, int levels) {
    return doComputeLineage(sourceDataset, startMillis, endMillis, levels, null);
  }

  private Lineage doComputeLineage(DatasetId sourceData,
                                   long startMillis, long endMillis,
                                   int levels, @Nullable String rollup) {
    LOG.trace("Computing lineage for data {}, startMillis {}, endMillis {}, levels {}",
              sourceData, startMillis, endMillis, levels);
    boolean rollUpWorkflow = rollup != null && rollup.contains("workflow");

    // Convert start time and end time period into scan keys in terms of program start times.
    Set<RunId> runningInRange = store.getRunningInRange(TimeUnit.MILLISECONDS.toSeconds(startMillis),
                                                        TimeUnit.MILLISECONDS.toSeconds(endMillis));
    LOG.trace("Got {} rundIds in time range ({}, {})", runningInRange.size(), startMillis, endMillis);

    ScanRangeWithFilter scanRange = getScanRange(runningInRange);
    LOG.trace("Using scan start = {}, scan end = {}", scanRange.getStart(), scanRange.getEnd());

    Multimap<RelationKey, Relation> relations = HashMultimap.create();
    Set<DatasetId> visitedDatasets = new HashSet<>();
    Set<DatasetId> toVisitDatasets = new HashSet<>();
    Set<ProgramId> visitedPrograms = new HashSet<>();
    Set<ProgramId> toVisitPrograms = new HashSet<>();
    // this map is to map the inner program run id to the workflow run id, this is needed to collapse the inner
    // program and local datasets
    Map<ProgramRunId, ProgramRunId> programWorkflowMap = new HashMap<>();

    toVisitDatasets.add(sourceData);
    for (int i = 0; i < levels; ++i) {
      LOG.trace("Level {}", i);
      toVisitPrograms.clear();
      for (DatasetId d : toVisitDatasets) {
        if (visitedDatasets.add(d)) {
          LOG.trace("Visiting dataset {}", d);
          // Fetch related programs, the programs will be the inner programs which access the datasets. For example,
          // mapreduce or spark program in a workflow
          Set<Relation> programRelations = lineageStoreReader.getRelations(d, scanRange.getStart(), scanRange.getEnd(),
                                                                           scanRange.getFilter());
          LOG.trace("Got program relations {}", programRelations);

          // if we want to roll up lineage for workflow, we need to figure out what workflow these programs are related
          // to and find out all the inner programs of that workflow, the workflow run id can also be used to
          // determine if a dataset is local dataset. The local dataset always ends with the workflow run id
          if (rollUpWorkflow) {
            computeWorkflowInnerPrograms(toVisitPrograms, programWorkflowMap, programRelations);
          }

          // add to the relations, replace the inner program with the workflow using the map, ignore the
          // local datasets relations, the local dataset always ends with the run id of the workflow
          filterAndAddRelations(rollUpWorkflow, relations, programWorkflowMap, programRelations);
          toVisitPrograms.addAll(programRelations.stream().map(Relation::getProgram).collect(Collectors.toSet()));
        }
      }

      toVisitDatasets.clear();
      for (ProgramId p : toVisitPrograms) {
        if (visitedPrograms.add(p)) {
          LOG.trace("Visiting program {}", p);
          // Fetch related datasets
          Set<Relation> datasetRelations = lineageStoreReader.getRelations(p, scanRange.getStart(),
                                                                           scanRange.getEnd(), scanRange.getFilter());
          LOG.trace("Got data relations {}", datasetRelations);
          Set<DatasetId> localDatasets = filterAndAddRelations(rollUpWorkflow, relations,
                                                               programWorkflowMap, datasetRelations);
          toVisitDatasets.addAll(
            datasetRelations.stream().map(relation -> (DatasetId) relation.getData())
              .filter(datasetId -> !localDatasets.contains(datasetId)).collect(Collectors.toSet()));
        }
      }
    }

    Lineage lineage = new Lineage(
      Iterables.concat(Maps.transformValues(relations.asMap(), COLLAPSE_UNKNOWN_TYPE_FUNCTION::apply).values()));
    LOG.trace("Got lineage {}", lineage);
    return lineage;
  }

  /**
   * Filter the relations based on the rollUp flag, if set to true, the method will replace the inner program with
   * the workflow using the map and ignore the local datasets relations. The local dataset always ends with the run
   * id of the workflow. The set of filtered local datasets is returned
   */
  private Set<DatasetId> filterAndAddRelations(boolean rollUpWorkflow, Multimap<RelationKey, Relation> relations,
                                               Map<ProgramRunId, ProgramRunId> programWorkflowMap,
                                               Set<Relation> relationss) {
    Set<DatasetId> localDatasets = new HashSet<>();
    for (Relation relation : relationss) {
      if (rollUpWorkflow && programWorkflowMap.containsKey(relation.getProgramRunId())) {
        ProgramRunId workflowId = programWorkflowMap.get(relation.getProgramRunId());
        // skip the relation for local datasets, local datasets always end with the workflow run id
        DatasetId data = (DatasetId) relation.getData();
        if (data.getDataset().endsWith(workflowId.getRun())) {
          localDatasets.add(data);
          continue;
        }
        relation = new Relation(data, workflowId.getParent(), relation.getAccess(),
                                RunIds.fromString(workflowId.getRun()));
      }
      relations.put(new RelationKey(relation), relation);
    }
    return localDatasets;
  }

  /**
   * Compute the inner programs and program runs based on the program relations and add them to the collections.
   *
   * @param toVisitPrograms the collection of next to visit programs
   * @param programWorkflowMap the program workflow run id map
   * @param programRelations the program relations of the dataset
   */
  private void computeWorkflowInnerPrograms(Set<ProgramId> toVisitPrograms,
                                            Map<ProgramRunId, ProgramRunId> programWorkflowMap,
                                            Set<Relation> programRelations) {
    // Step 1 walk through the program relations, filter out the possible mapreduce and spark programs that
    // could be in the workflow, and get the appSpec for the program, to determine what other programs
    // are in the workflow
    Map<ApplicationId, ApplicationSpecification> appSpecs = new HashMap<>();
    Set<ProgramRunId> possibleInnerPrograms = new HashSet<>();
    programRelations.forEach(relation -> {
      ProgramType type = relation.getProgram().getType();
      if (type.equals(ProgramType.MAPREDUCE) || type.equals(ProgramType.SPARK)) {
        possibleInnerPrograms.add(relation.getProgramRunId());
        appSpecs.computeIfAbsent(relation.getProgram().getParent(), store::getApplication);
      }
    });

    // Step 2, get the run record for all the possible inner programs, the run record contains the
    // workflow information, fetch the workflow id and add them to the map
    Map<ProgramRunId, RunRecordDetail> runRecords = store.getRuns(possibleInnerPrograms);
    Set<ProgramRunId> workflowRunIds = new HashSet<>();
    runRecords.entrySet().stream()
      .filter(e -> e.getValue() != null)
      .forEach(entry -> {
        ProgramRunId programRunId = entry.getKey();
        RunRecordDetail runRecord = entry.getValue();

        if (runRecord.getSystemArgs().containsKey(ProgramOptionConstants.WORKFLOW_RUN_ID)) {
          ProgramRunId wfRunId = extractWorkflowRunId(programRunId, runRecord);
          programWorkflowMap.put(programRunId, wfRunId);
          workflowRunIds.add(wfRunId);
        }
      }
    );

    // Step 3, fetch run records of the workflow, the properties of the workflow run record has all
    // the inner program run ids, compare them with the app spec to get the type of the program
    runRecords = store.getRuns(workflowRunIds);
    runRecords.entrySet().stream()
      .filter(e -> e.getValue() != null)
      .forEach(entry -> {
        ProgramRunId programRunId = entry.getKey();
        RunRecordDetail runRecord = entry.getValue();
        extractAndAddInnerPrograms(toVisitPrograms, programWorkflowMap, appSpecs, programRunId, runRecord);
      });
  }

  private ProgramRunId extractWorkflowRunId(ProgramRunId programRunId, RunRecordDetail runRecord) {
    Map<String, String> systemArgs = runRecord.getSystemArgs();
    String workflowRunId = systemArgs.get(ProgramOptionConstants.WORKFLOW_RUN_ID);
    String workflowName = systemArgs.get(ProgramOptionConstants.WORKFLOW_NAME);
    WorkflowId workflowId = programRunId.getParent().getParent().workflow(workflowName);
    return workflowId.run(workflowRunId);
  }

  /**
   * Extract inner programs and runs from the workflow run record, the run record's properties have all the
   * inner program run ids. The workflow spec can then be used to determine what the inner programs are and
   * create the program run ids for them
   */
  private void extractAndAddInnerPrograms(Set<ProgramId> toVisitPrograms,
                                          Map<ProgramRunId, ProgramRunId> programWorkflowMap,
                                          Map<ApplicationId, ApplicationSpecification> appSpecs,
                                          ProgramRunId programRunId, RunRecordDetail wfRunRecord) {
    ApplicationId appId = programRunId.getParent().getParent();
    WorkflowSpecification workflowSpec =
      appSpecs.get(appId).getWorkflows().get(programRunId.getProgram());
    Map<String, WorkflowNode> nodeIdMap = workflowSpec.getNodeIdMap();
    wfRunRecord.getProperties().forEach((key, value) -> {
      if (nodeIdMap.containsKey(key)) {
        WorkflowActionNode node = (WorkflowActionNode) nodeIdMap.get(key);
        ProgramType type = ProgramType.valueOf(node.getProgram().getProgramType().name());
        ProgramId program = appId.program(type, key);
        programWorkflowMap.put(program.run(value), programRunId);
        toVisitPrograms.add(program);
      }
    });
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
      return new ScanRangeWithFilter(0, 0, x -> false);
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
    return new ScanRangeWithFilter(earliest, latest + 1, input -> runIds.contains(input.getRun()));
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

    public Relation getRelation() {
      return relation;
    }
  }
}
