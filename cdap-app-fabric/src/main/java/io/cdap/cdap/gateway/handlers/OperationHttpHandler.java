/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers;

import com.google.gson.Gson;
import com.google.inject.Inject;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.AppFabric;
import io.cdap.cdap.common.lang.Exceptions;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.internal.operation.OperationLifecycleManager;
import io.cdap.cdap.internal.operation.OperationRunFilter;
import io.cdap.cdap.internal.operation.OperationRunNotFoundException;
import io.cdap.cdap.internal.operation.ScanOperationRunsRequest;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operation.OperationRun;
import io.cdap.cdap.proto.operation.OperationRunStatus;
import io.cdap.cdap.proto.operation.OperationType;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/** The {@link HttpHandler} for handling REST calls to operation endpoints. */
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}/operations")
public class OperationHttpHandler extends AbstractAppFabricHttpHandler {
  private final CConfiguration cConf;
  private static final Gson GSON = new Gson();
  private final OperationLifecycleManager operationLifecycleManager;
  private final int batchSize;
  public static final String OPERATIONS_LIST_PAGINATED_KEY = "operations";

  @Inject
  OperationHttpHandler(CConfiguration cConf, OperationLifecycleManager operationLifecycleManager) throws Exception {
    this.cConf = cConf;
    this.batchSize = this.cConf.getInt(AppFabric.STREAMING_BATCH_SIZE);
    this.operationLifecycleManager = operationLifecycleManager;
  }

  /**
   * API to fetch all running operations in a namespace.
   *
   * @param namespaceId Namespace to fetch runs from
   * @param pageToken the token identifier for the current page requested in a paginated request
   * @param pageSize the number of application details returned in a paginated request
   * @param filter optional filters in EBNF grammar. Currently Only one status and one type filter
   *     is supported with AND expression.
   */
  @GET
  @Path("/")
  public void scanOperations(
      HttpRequest request,
      HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @QueryParam("pageToken") String pageToken,
      @QueryParam("pageSize") Integer pageSize,
      @QueryParam("filter") String filter)
      throws BadRequestException, IOException {
    validateNamespace(namespaceId);
    JsonPaginatedListResponder.respond(
        GSON,
        responder,
        OPERATIONS_LIST_PAGINATED_KEY,
        jsonListResponder -> {
          AtomicReference<OperationRun> lastRun = new AtomicReference<>(null);
          ScanOperationRunsRequest scanRequest =
              getScanRequest(namespaceId, pageToken, pageSize, filter);
          boolean pageLimitReached = false;
          try {
            pageLimitReached =
                operationLifecycleManager.scanOperations(
                    scanRequest,
                    batchSize,
                    runDetail -> {
                      OperationRun run = runDetail.getRun();
                      jsonListResponder.send(run);
                      lastRun.set(run);
                    });
          } catch (IOException e) {
            responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
          } catch (OperationRunNotFoundException e) {
            responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
          }
          OperationRun run = lastRun.get();
          return !pageLimitReached || run == null ? null : run.getId();
        });
  }

  private ScanOperationRunsRequest getScanRequest(
      String namespaceId, String pageToken, Integer pageSize, String filter) {
    ScanOperationRunsRequest.Builder builder = ScanOperationRunsRequest.builder();
    builder.setNamespace(namespaceId);
    if (pageSize != null) {
      builder.setLimit(pageSize);
    }
    if (pageToken != null) {
      builder.setScanAfter(pageToken);
    }
    if (filter != null && !filter.isEmpty()) {
      OperationType operationType = null;
      OperationRunStatus operationStatus = null;
      Pattern typePattern = Pattern.compile("type=(\\w+)");
      Pattern statusPattern = Pattern.compile("status=(\\w+)");
      Matcher typeMatcher = typePattern.matcher(filter);
      Matcher statusMatcher = statusPattern.matcher(filter);
      while (typeMatcher.find()) {
        String typeStr = typeMatcher.group(1);
        operationType = OperationType.valueOf(typeStr.toUpperCase());
      }
      while (statusMatcher.find()) {
        String statusStr = statusMatcher.group(1);
        operationStatus = OperationRunStatus.valueOf(statusStr.toUpperCase());
      }
      builder.setFilter(new OperationRunFilter(operationType, operationStatus));
    }

    return builder.build();
  }

  private void validateNamespace(@Nullable String namespaceId) throws BadRequestException {
    if (namespaceId == null) {
      throw new BadRequestException("Path parameter namespaceId cannot be empty");
    }
  }

  /**
   * API to fetch operation run by id.
   *
   * @param namespaceId Namespace to fetch runs from
   * @param runId id of the operation run
   */
  @GET
  @Path("/{id}")
  public void getOperationRun(
      HttpRequest request,
      HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("id") String runId)
      throws BadRequestException, OperationRunNotFoundException, IOException {
    validateNamespace(namespaceId);
    if (runId == null || runId.isEmpty()) {
      throw new BadRequestException("Path parameter runId cannot be empty");
    }
    responder.sendJson(
        HttpResponseStatus.OK,
        GSON.toJson(
            operationLifecycleManager
                .getOperationRun(new OperationRunId(namespaceId, runId))
                .getRun()));
  }

  /**
   * API to stop operation run by id.
   *
   * @param namespaceId Namespace to fetch runs from
   * @param runId id of the operation run
   */
  @POST
  @Path("/{id}/stop")
  public void failOperation(
      FullHttpRequest request,
      HttpResponder responder,
      @PathParam("namespace-id") String namespaceId,
      @PathParam("id") String runId) {
    // // TODO(samik, CDAP-20814) send the message to stop the operation
    responder.sendString(
        HttpResponseStatus.OK,
        String.format(
            "Updated status for operation run %s in namespace '%s'.", runId, namespaceId));
  }
}
