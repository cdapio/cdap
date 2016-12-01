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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricSearchQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.TagValue;
import co.cask.cdap.app.mapreduce.MRJobInfoFetcher;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.TimeMathParser;
import co.cask.cdap.internal.app.store.WorkflowDataset;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.WorkflowStatistics;
import co.cask.cdap.proto.WorkflowStatsComparison;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.WorkflowId;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Workflow Statistics Handler
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class WorkflowStatsSLAHttpHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowStatsSLAHttpHandler.class);
  private final Store store;
  private final MRJobInfoFetcher mrJobInfoFetcher;
  private final MetricStore metricStore;

  @Inject
  WorkflowStatsSLAHttpHandler(Store store, MRJobInfoFetcher mrJobInfoFetcher, MetricStore metricStore) {
    this.store = store;
    this.mrJobInfoFetcher = mrJobInfoFetcher;
    this.metricStore = metricStore;
  }

  /**
   * Returns the statistics for a given workflow.
   *
   * @param request The request
   * @param responder The responder
   * @param namespaceId The namespace the application is in
   * @param appId The application the workflow is in
   * @param workflowId The workflow that needs to have it stats shown
   * @param start The start time of the range
   * @param end The end time of the range
   * @param percentiles The list of percentile values on which visibility is needed
   */
  @GET
  @Path("apps/{app-id}/workflows/{workflow-id}/statistics")
  public void workflowStats(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("app-id") String appId,
                            @PathParam("workflow-id") String workflowId,
                            @QueryParam("start") @DefaultValue("now-1d") String start,
                            @QueryParam("end") @DefaultValue("now") String end,
                            @QueryParam("percentile") @DefaultValue("90.0") List<Double> percentiles) throws Exception {
    long startTime = TimeMathParser.parseTimeInSeconds(start);
    long endTime = TimeMathParser.parseTimeInSeconds(end);

    if (startTime < 0) {
      throw new BadRequestException("Invalid start time. The time you entered was : " + startTime);
    } else if (endTime < 0) {
      throw new BadRequestException("Invalid end time. The time you entered was : " + endTime);
    } else if (endTime < startTime) {
      throw new BadRequestException("Start time : " + startTime + " cannot be larger than end time : " + endTime);
    }

    for (double i : percentiles) {
      if (i < 0.0 || i > 100.0) {
        throw new BadRequestException("Percentile values have to be greater than or equal to 0 and" +
                                        " less than or equal to 100. Invalid input was " + Double.toString(i));
      }
    }

    WorkflowId workflow = new WorkflowId(namespaceId, appId, workflowId);
    WorkflowStatistics workflowStatistics = store.getWorkflowStatistics(workflow, startTime, endTime, percentiles);

    if (workflowStatistics == null) {
      responder.sendString(HttpResponseStatus.OK, "There are no statistics associated with this workflow : "
        + workflowId + " in the specified time range.");
      return;
    }
    responder.sendJson(HttpResponseStatus.OK, workflowStatistics);
  }

  /**
   * The endpoint returns a list of workflow metrics based on the workflow run and a surrounding number of runs
   * of the workflow that are spaced apart by a time interval from each other.
   *
   * @param request The request
   * @param responder The responder
   * @param namespaceId The namespace the application is in
   * @param appId The application the workflow is in
   * @param workflowId The workflow that needs to have it stats shown
   * @param runId The run id of the Workflow that the user wants to see
   * @param limit The number of the records that the user wants to compare against on either side of the run
   * @param interval The timeInterval with which the user wants to space out the runs
   */
  @GET
  @Path("apps/{app-id}/workflows/{workflow-id}/runs/{run-id}/statistics")
  public void workflowRunDetail(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("app-id") String appId,
                                @PathParam("workflow-id") String workflowId,
                                @PathParam("run-id") String runId,
                                @QueryParam("limit") @DefaultValue("10") int limit,
                                @QueryParam("interval") @DefaultValue("10s") String interval) throws Exception {
    if (limit <= 0) {
      throw new BadRequestException("Limit has to be greater than 0. Entered value was : " + limit);
    }

    long timeInterval;
    try {
      timeInterval = TimeMathParser.resolutionInSeconds(interval);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException("Interval is specified with invalid time unit. It should be specified with one" +
                                      " of the 'ms', 's', 'm', 'h', 'd' units. Entered value was : " + interval);
    }

    if (timeInterval <= 0) {
      throw new BadRequestException("Interval should be greater than 0 and should be specified with one of the 'ms'," +
                                      " 's', 'm', 'h', 'd' units. Entered value was : " + interval);
    }
    WorkflowId workflow = new WorkflowId(namespaceId, appId, workflowId);
    Collection<WorkflowDataset.WorkflowRunRecord> workflowRunRecords =
      store.retrieveSpacedRecords(workflow, runId, limit, timeInterval);

    List<WorkflowRunMetrics> workflowRunMetricsList = new ArrayList<>();
    Map<String, Long> startTimes = new HashMap<>();
    for (WorkflowDataset.WorkflowRunRecord workflowRunRecord : workflowRunRecords) {
      workflowRunMetricsList.add(getDetailedRecord(workflow, workflowRunRecord.getWorkflowRunId()));
      startTimes.put(workflowRunRecord.getWorkflowRunId(),
                     RunIds.getTime(RunIds.fromString(workflowRunRecord.getWorkflowRunId()), TimeUnit.SECONDS));
    }

    Collection<WorkflowStatsComparison.ProgramNodes> formattedStatisticsMap = format(workflowRunMetricsList);
    responder.sendJson(HttpResponseStatus.OK, new WorkflowStatsComparison(startTimes, formattedStatisticsMap));
  }

  /**
   * Compare the metrics of 2 runs of a workflow
   *
   * @param request The request
   * @param responder The responder
   * @param namespaceId The namespace the application is in
   * @param appId The application the workflow is in
   * @param workflowId The workflow that needs to have it stats shown
   * @param runId The run id of the Workflow that the user wants to see
   * @param otherRunId The other run id of the same workflow that the user wants to compare against
   */
  @GET
  @Path("apps/{app-id}/workflows/{workflow-id}/runs/{run-id}/compare")
  public void compare(HttpRequest request, HttpResponder responder,
                      @PathParam("namespace-id") String namespaceId,
                      @PathParam("app-id") String appId,
                      @PathParam("workflow-id") String workflowId,
                      @PathParam("run-id") String runId,
                      @QueryParam("other-run-id") String otherRunId) throws Exception {
    WorkflowId workflow = new WorkflowId(namespaceId, appId, workflowId);
    WorkflowRunMetrics detailedStatistics = getDetailedRecord(workflow, runId);
    WorkflowRunMetrics otherDetailedStatistics = getDetailedRecord(workflow, otherRunId);
    if (detailedStatistics == null) {
      throw new NotFoundException("The run-id provided was not found : " + runId);
    }
    if (otherDetailedStatistics == null) {
      throw new NotFoundException("The other run-id provided was not found : " + otherRunId);
    }

    List<WorkflowRunMetrics> workflowRunMetricsList = new ArrayList<>();
    workflowRunMetricsList.add(detailedStatistics);
    workflowRunMetricsList.add(otherDetailedStatistics);
    responder.sendJson(HttpResponseStatus.OK, format(workflowRunMetricsList));
  }

  private Collection<WorkflowStatsComparison.ProgramNodes> format(List<WorkflowRunMetrics> workflowRunMetricsList) {
    Map<String, WorkflowStatsComparison.ProgramNodes> programLevelDetails = new HashMap<>();
    for (WorkflowRunMetrics workflowRunMetrics : workflowRunMetricsList) {
      for (ProgramMetrics programMetrics : workflowRunMetrics.getProgramMetricsList()) {
        String programName = programMetrics.getProgramName();
        if (programLevelDetails.get(programName) == null) {
          WorkflowStatsComparison.ProgramNodes programNodes = new WorkflowStatsComparison.ProgramNodes(
            programName, programMetrics.getProgramType(),
            new ArrayList<WorkflowStatsComparison.ProgramNodes.WorkflowProgramDetails>());
          programLevelDetails.put(programName, programNodes);
        }
        programLevelDetails.get(programName).addWorkflowDetails(
          workflowRunMetrics.getWorkflowRunId(), programMetrics.getProgramRunId(),
          programMetrics.getProgramStartTime(), programMetrics.getMetrics());
      }
    }
    return programLevelDetails.values();
  }

  /**
   * Returns the detailed Record for the Workflow
   *
   * @param workflowId Workflow that needs to get its detailed record
   * @param runId Run Id of the workflow
   * @return Return the Workflow Run Metrics
   */
  @Nullable
  private WorkflowRunMetrics getDetailedRecord(WorkflowId workflowId, String runId) throws Exception {
    WorkflowDataset.WorkflowRunRecord workflowRunRecord = store.getWorkflowRun(workflowId, runId);
    if (workflowRunRecord == null) {
      return null;
    }
    List<WorkflowDataset.ProgramRun> programRuns = workflowRunRecord.getProgramRuns();
    List<ProgramMetrics> programMetricsList = new ArrayList<>();
    for (WorkflowDataset.ProgramRun programRun : programRuns) {
      Map<String, Long> programMap = new HashMap<>();
      String programName = programRun.getName();
      ProgramType programType = programRun.getProgramType();
      ProgramId program = new ProgramId(workflowId.getNamespace(), workflowId.getApplication(),
                                        programType, programName);
      String programRunId = programRun.getRunId();
      if (programType == ProgramType.MAPREDUCE) {
        programMap = getMapreduceDetails(program, programRunId);
      } else if (programType == ProgramType.SPARK) {
        programMap = getSparkDetails(program, programRunId);
      }
      programMap.put("timeTaken", programRun.getTimeTaken());
      long programStartTime = RunIds.getTime(RunIds.fromString(programRunId), TimeUnit.SECONDS);
      programMetricsList.add(new ProgramMetrics(programName, programType, programRunId, programStartTime, programMap));
    }
    return new WorkflowRunMetrics(runId, programMetricsList);
  }

  private Map<String, Long> getMapreduceDetails(ProgramId mapreduceProgram, String runId) throws Exception {
    return mrJobInfoFetcher.getMRJobInfo(mapreduceProgram.run(runId).toId()).getCounters();
  }

  private Map<String, Long> getSparkDetails(ProgramId sparkProgram, String runId) throws Exception {
    Map<String, String> context = new HashMap<>();
    context.put(Constants.Metrics.Tag.NAMESPACE, sparkProgram.getNamespace());
    context.put(Constants.Metrics.Tag.APP, sparkProgram.getApplication());
    context.put(Constants.Metrics.Tag.SPARK, sparkProgram.getProgram());
    context.put(Constants.Metrics.Tag.RUN_ID, runId);

    List<TagValue> tags = new ArrayList<>();
    for (Map.Entry<String, String> entry : context.entrySet()) {
      tags.add(new TagValue(entry.getKey(), entry.getValue()));
    }
    MetricSearchQuery metricSearchQuery = new MetricSearchQuery(0, 0, Integer.MAX_VALUE, tags);
    Collection<String> metricNames = metricStore.findMetricNames(metricSearchQuery);
    Map<String, Long> overallResult = new HashMap<>();
    for (String metricName : metricNames) {
      Collection<MetricTimeSeries> resultPerQuery = metricStore.query(
        new MetricDataQuery(0, 0, Integer.MAX_VALUE, metricName, AggregationFunction.SUM,
                            context, new ArrayList<String>()));

      for (MetricTimeSeries metricTimeSeries : resultPerQuery) {
        overallResult.put(metricTimeSeries.getMetricName(), metricTimeSeries.getTimeValues().get(0).getValue());
      }
    }
    return overallResult;
  }

  private static class ProgramMetrics {
    private final String programName;
    private final ProgramType programType;
    private final String programRunId;
    private final long programStartTime;
    private final Map<String, Long> metrics;

    private ProgramMetrics(String programName, ProgramType programType, String programRunId,
                          long programStartTime, Map<String, Long> metrics) {
      this.programName = programName;
      this.programType = programType;
      this.programRunId = programRunId;
      this.programStartTime = programStartTime;
      this.metrics = metrics;
    }

    public String getProgramName() {
      return programName;
    }

    public ProgramType getProgramType() {
      return programType;
    }

    public Map<String, Long> getMetrics() {
      return metrics;
    }

    public long getProgramStartTime() {
      return programStartTime;
    }

    public String getProgramRunId() {
      return programRunId;
    }
  }

  private static class WorkflowRunMetrics {
    private final String workflowRunId;
    private final List<ProgramMetrics> programMetricsList;

    private WorkflowRunMetrics(String workflowRunId, List<ProgramMetrics> programMetricsList) {
      this.workflowRunId = workflowRunId;
      this.programMetricsList = programMetricsList;
    }

    public String getWorkflowRunId() {
      return workflowRunId;
    }

    public List<ProgramMetrics> getProgramMetricsList() {
      return programMetricsList;
    }
  }
}
