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

import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.TimeMathParser;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.WorkflowStatistics;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
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

  @Inject
  WorkflowStatsSLAHttpHandler(Store store) {
    this.store = store;
  }

  @GET
  @Path("apps/{app-id}/workflows/{workflow-id}/statistics")
  public void workflowStats(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("app-id") String appId,
                            @PathParam("workflow-id") String workflowId,
                            @QueryParam("start") String start,
                            @QueryParam("end") String end,
                            @QueryParam("percentile") List<Double> percentiles) throws Exception {
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

    Id.Workflow workflow = Id.Workflow.from(Id.Namespace.from(namespaceId), appId, workflowId);
    WorkflowStatistics workflowStatistics = store.getWorkflowStatistics(workflow, startTime, endTime, percentiles);

    if (workflowStatistics == null) {
      responder.sendString(HttpResponseStatus.OK, "There are no statistics associated with this workflow : "
        + workflowId + " in the specified time range.");
      return;
    }
    responder.sendJson(HttpResponseStatus.OK, workflowStatistics);
  }
}
