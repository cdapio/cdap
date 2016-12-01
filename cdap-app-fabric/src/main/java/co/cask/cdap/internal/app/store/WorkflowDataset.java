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

package co.cask.cdap.internal.app.store;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.proto.PercentileInformation;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.WorkflowStatistics;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.WorkflowId;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.twill.api.RunId;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Dataset for Completed Workflows and their associated programs
 */
public class WorkflowDataset extends AbstractDataset {

  private static final Gson GSON = new Gson();
  private static final byte[] RUNID = Bytes.toBytes("r");
  private static final byte[] TIME_TAKEN = Bytes.toBytes("t");
  private static final byte[] NODES = Bytes.toBytes("n");
  private static final Type PROGRAM_RUNS_TYPE = new TypeToken<List<ProgramRun>>() { }.getType();

  private final Table table;

  WorkflowDataset(Table table) {
    super("workflow.statistics", table);
    this.table = table;
  }

  void write(WorkflowId id, RunRecordMeta runRecordMeta, List<ProgramRun> programRunList) {
    long startTs = runRecordMeta.getStartTs();

    MDSKey mdsKey = getRowKeyBuilder(id, startTs).build();
    byte[] rowKey = mdsKey.getKey();
    Long stopTs = runRecordMeta.getStopTs();
    Preconditions.checkState(stopTs != null, "Workflow Stats are written when the workflow has completed. Hence, " +
      "expected workflow stop time to be non-null. Workflow = %s, Run = %s, Stop time = %s", id, runRecordMeta, stopTs);
    long timeTaken = stopTs - startTs;

    String value = GSON.toJson(programRunList, PROGRAM_RUNS_TYPE);

    table.put(rowKey, RUNID, Bytes.toBytes(runRecordMeta.getPid()));
    table.put(rowKey, TIME_TAKEN, Bytes.toBytes(timeTaken));
    table.put(rowKey, NODES, Bytes.toBytes(value));
  }

  public void delete(ApplicationId id) {
    MDSKey mdsKey = new MDSKey.Builder().add(id.getNamespace()).add(id.getApplication()).build();
    Scanner scanner = table.scan(mdsKey.getKey(), Bytes.stopKeyForPrefix(mdsKey.getKey()));
    Row row;
    try {
      while ((row = scanner.next()) != null) {
        table.delete(row.getRow());
      }
    } finally {
      scanner.close();
    }
  }

  /**
   * This function scans the workflow.stats dataset for a list of workflow runs in a time range.
   *
   * @param id The workflow id
   * @param timeRangeStart Start of the time range that the scan should begin from
   * @param timeRangeEnd End of the time range that the scan should end at
   * @return List of WorkflowRunRecords
   */
  private List<WorkflowRunRecord> scan(WorkflowId id, long timeRangeStart, long timeRangeEnd) {
    byte[] startRowKey = getRowKeyBuilder(id, timeRangeStart).build().getKey();
    byte[] endRowKey = getRowKeyBuilder(id, timeRangeEnd).build().getKey();
    Scan scan = new Scan(startRowKey, endRowKey);

    Scanner scanner = table.scan(scan);
    Row indexRow;
    List<WorkflowRunRecord> workflowRunRecordList = new ArrayList<>();
    while ((indexRow = scanner.next()) != null) {
      Map<byte[], byte[]> columns = indexRow.getColumns();
      String workflowRunId = Bytes.toString(columns.get(RUNID));
      long timeTaken = Bytes.toLong(columns.get(TIME_TAKEN));

      List<ProgramRun> programRunList = GSON.fromJson(Bytes.toString(columns.get(NODES)), PROGRAM_RUNS_TYPE);
      WorkflowRunRecord workflowRunRecord = new WorkflowRunRecord(workflowRunId, timeTaken, programRunList);
      workflowRunRecordList.add(workflowRunRecord);
    }
    return workflowRunRecordList;
  }

  /**
   * This method returns the statistics for a corresponding workflow. The user has to
   * provide a time interval and a list of percentiles that are required.
   *
   * @param id The workflow id
   * @param startTime The start of the time range from where the user wants the statistics
   * @param endTime The end of the time range till where the user wants the statistics
   * @param percentiles The list of percentiles that the user wants information on
   * @return A statistics object that provides information about the workflow or null if there are no runs associated
   * with the workflow
   * @throws Exception
   */
  @Nullable
  public WorkflowStatistics getStatistics(WorkflowId id, long startTime,
                                          long endTime, List<Double> percentiles) throws Exception {
    List<WorkflowRunRecord> workflowRunRecords = scan(id, startTime, endTime);
    int runs = workflowRunRecords.size();

    if (runs == 0) {
      return null;
    }

    double avgRunTime = 0.0;
    for (WorkflowDataset.WorkflowRunRecord workflowRunRecord : workflowRunRecords) {
      avgRunTime += workflowRunRecord.getTimeTaken();
    }
    avgRunTime /= runs;

    workflowRunRecords = sort(workflowRunRecords);

    List<PercentileInformation> percentileInformationList = getPercentiles(workflowRunRecords, percentiles);

    Collection<ProgramRunDetails> programToRunRecord = getProgramRuns(workflowRunRecords);

    Map<String, Map<String, String>> programToStatistic = new HashMap<>();
    for (ProgramRunDetails entry : programToRunRecord) {
      double avgForProgram = 0;
      for (long value : entry.getProgramRunList()) {
        avgForProgram += value;
      }
      avgForProgram /= entry.getProgramRunList().size();
      Map<String, String> programMap = new HashMap<>();
      programMap.put("type", entry.getProgramType().toString());
      programMap.put("runs", Long.toString(entry.getProgramRunList().size()));
      programMap.put("avgRunTime", Double.toString(avgForProgram));
      programToStatistic.put(entry.getName(), programMap);
      List<Long> runList = entry.getProgramRunList();
      Collections.sort(runList);
      for (double percentile : percentiles) {
        long percentileValue = runList.get((int) ((percentile * runList.size()) / 100));
        programToStatistic.get(entry.getName()).put(Double.toString(percentile), Long.toString(percentileValue));
      }
    }

    return new WorkflowStatistics(startTime, endTime, runs, avgRunTime, percentileInformationList,
                                  programToStatistic);
  }

  private List<PercentileInformation> getPercentiles(List<WorkflowRunRecord> workflowRunRecords,
                                                     List<Double> percentiles) {
    int runs = workflowRunRecords.size();
    List<PercentileInformation> percentileInformationList = new ArrayList<>();
    for (double i : percentiles) {
      List<String> percentileRun = new ArrayList<>();
      int percentileStart = (int) ((i * runs) / 100);
      for (int j = percentileStart; j < runs; j++) {
        percentileRun.add(workflowRunRecords.get(j).getWorkflowRunId());
      }
      percentileInformationList.add(
        new PercentileInformation(i, workflowRunRecords.get(percentileStart).getTimeTaken(), percentileRun));
    }
    return percentileInformationList;
  }

  private List<WorkflowDataset.WorkflowRunRecord> sort(List<WorkflowDataset.WorkflowRunRecord> workflowRunRecords) {
    Collections.sort(workflowRunRecords, new Comparator<WorkflowRunRecord>() {
      @Override
      public int compare(WorkflowDataset.WorkflowRunRecord o1, WorkflowDataset.WorkflowRunRecord o2) {
        return Longs.compare(o1.getTimeTaken(), o2.getTimeTaken());
      }
    });
    return workflowRunRecords;
  }

  private Collection<ProgramRunDetails> getProgramRuns(List<WorkflowDataset.WorkflowRunRecord> workflowRunRecords) {
    Map<String, ProgramRunDetails> programToRunRecord = new HashMap<>();
    for (WorkflowDataset.WorkflowRunRecord workflowRunRecord : workflowRunRecords) {
      for (WorkflowDataset.ProgramRun run : workflowRunRecord.getProgramRuns()) {
        ProgramRunDetails programRunDetails = programToRunRecord.get(run.getName());
        if (programRunDetails == null) {
          programRunDetails = new ProgramRunDetails(run.getName(), run.getProgramType(), new ArrayList<Long>());
          programToRunRecord.put(run.getName(), programRunDetails);
        }
        programRunDetails.addToProgramRunList(run.getTimeTaken());
      }
    }
    return programToRunRecord.values();
  }

  @Nullable
  WorkflowRunRecord getRecord(WorkflowId id, String pid) {
    RunId runId = RunIds.fromString(pid);
    long startTime = RunIds.getTime(runId, TimeUnit.SECONDS);
    MDSKey mdsKey = getRowKeyBuilder(id, startTime).build();
    byte[] startRowKey = mdsKey.getKey();

    Row indexRow = table.get(startRowKey);
    if (indexRow.isEmpty()) {
      return null;
    }
    Map<byte[], byte[]> columns = indexRow.getColumns();
    String workflowRunId = Bytes.toString(columns.get(RUNID));
    long timeTaken = Bytes.toLong(columns.get(TIME_TAKEN));

    List<ProgramRun> actionRunsList = GSON.fromJson(Bytes.toString(columns.get(NODES)), PROGRAM_RUNS_TYPE);
    return new WorkflowRunRecord(workflowRunId, timeTaken, actionRunsList);
  }

  Collection<WorkflowRunRecord> getDetailsOfRange(WorkflowId workflow, String runId, int limit, long timeInterval) {
    Map<String, WorkflowRunRecord> mainRunRecords = getNeighbors(workflow, RunIds.fromString(runId),
                                                                 limit, timeInterval);
    WorkflowRunRecord workflowRunRecord = getRecord(workflow, runId);
    if (workflowRunRecord != null) {
      mainRunRecords.put(workflowRunRecord.getWorkflowRunId(), workflowRunRecord);
    }
    return mainRunRecords.values();
  }

  /**
   * Returns a map of WorkflowRunId to WorkflowRunRecord that are close to the WorkflowRunId provided by the user.
   *
   * @param id The workflow
   * @param runId The runid of the workflow
   * @param limit The limit on each side of the run that we want to see into
   * @param timeInterval The time interval that we want the results to be spaced apart
   * @return A Map of WorkflowRunId to the corresponding Workflow Run Record. A map is used so that duplicates of
   * the WorkflowRunRecord are not obtained
   */
  private Map<String, WorkflowRunRecord> getNeighbors(WorkflowId id, RunId runId, int limit, long timeInterval) {
    long startTime = RunIds.getTime(runId, TimeUnit.SECONDS);
    Map<String, WorkflowRunRecord> workflowRunRecords = new HashMap<>();
    int i = -limit;
    long prevStartTime = startTime - (limit * timeInterval);
    // The loop iterates across the range that is startTime - (limit * timeInterval) to
    // startTime + (limit * timeInterval) since we want to capture all runs that started in this range.
    // Since we want to stop getting the same key, we have the prevStartTime become 1 more than the time at which
    // the last record was found if the (interval * the count of the loop) is less than the time.
    while (prevStartTime <= startTime + (limit * timeInterval)) {
      MDSKey mdsKey = getRowKeyBuilder(id, prevStartTime).build();
      byte[] startRowKey = mdsKey.getKey();
      Scan scan = new Scan(startRowKey, null);
      Scanner scanner = table.scan(scan);
      Row indexRow = scanner.next();
      if (indexRow == null) {
        return workflowRunRecords;
      }
      byte[] rowKey = indexRow.getRow();
      long time = ByteBuffer.wrap(rowKey, rowKey.length - Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG).getLong();
      if (!((time >= (startTime - (limit * timeInterval))) && time <= (startTime + (limit * timeInterval)))) {
        break;
      }
      Map<byte[], byte[]> columns = indexRow.getColumns();
      String workflowRunId = Bytes.toString(columns.get(RUNID));
      long timeTaken = Bytes.toLong(columns.get(TIME_TAKEN));
      List<ProgramRun> programRunList = GSON.fromJson(Bytes.toString(columns.get(NODES)), PROGRAM_RUNS_TYPE);
      workflowRunRecords.put(workflowRunId, new WorkflowRunRecord(workflowRunId, timeTaken, programRunList));
      prevStartTime = startTime + (i * timeInterval) < time ?
        time + 1 : startTime + (i * timeInterval);
      i++;
    }
    return workflowRunRecords;
  }

  /**
   * Class to store the name, type and list of runs of the programs across all workflow runs
   */
  private static final class ProgramRunDetails {
    private final String name;
    private final ProgramType programType;
    private final List<Long> programRunList;

    ProgramRunDetails(String name, ProgramType programType, List<Long> programRunList) {
      this.name = name;
      this.programType = programType;
      this.programRunList = programRunList;
    }

    public void addToProgramRunList(long time) {
      this.programRunList.add(time);
    }

    public String getName() {
      return name;
    }

    public ProgramType getProgramType() {
      return programType;
    }

    public List<Long> getProgramRunList() {
      return programRunList;
    }
  }

  /**
   * Class to keep track of Workflow Run Records
   */
  public static final class WorkflowRunRecord {
    private final String workflowRunId;
    private final long timeTaken;
    private final List<ProgramRun> programRuns;

    public WorkflowRunRecord(String workflowRunId, long timeTaken, List<ProgramRun> programRuns) {
      this.programRuns = programRuns;
      this.timeTaken = timeTaken;
      this.workflowRunId = workflowRunId;
    }

    public long getTimeTaken() {
      return timeTaken;
    }

    public List<ProgramRun> getProgramRuns() {
      return programRuns;
    }

    public String getWorkflowRunId() {
      return workflowRunId;
    }
  }

  /**
   * Class for keeping track of programs in a workflow
   */
  public static final class ProgramRun {
    private final String runId;
    private final long timeTaken;
    private final ProgramType programType;
    private final String name;

    public ProgramRun(String name, String runId, ProgramType programType, long timeTaken) {
      this.name = name;
      this.runId = runId;
      this.programType = programType;
      this.timeTaken = timeTaken;
    }

    public ProgramType getProgramType() {
      return programType;
    }

    public long getTimeTaken() {
      return timeTaken;
    }

    public String getName() {
      return name;
    }

    public String getRunId() {
      return runId;
    }
    }

  private static MDSKey.Builder getRowKeyBuilder(WorkflowId id, long time) {
    return new MDSKey.Builder().add(id.getNamespace())
      .add(id.getApplication()).add(id.getVersion()).add(id.getProgram()).add(time);
  }
}
