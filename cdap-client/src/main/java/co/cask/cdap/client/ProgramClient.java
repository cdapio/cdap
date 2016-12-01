/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.client;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.customaction.CustomActionSpecification;
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.ApplicationRecord;
import co.cask.cdap.proto.BatchProgram;
import co.cask.cdap.proto.BatchProgramResult;
import co.cask.cdap.proto.BatchProgramStart;
import co.cask.cdap.proto.BatchProgramStatus;
import co.cask.cdap.proto.DistributedProgramLiveInfo;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.Instances;
import co.cask.cdap.proto.ProgramLiveInfo;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramStatus;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.codec.CustomActionSpecificationCodec;
import co.cask.cdap.proto.codec.WorkflowActionSpecificationCodec;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * Provides ways to interact with CDAP Programs.
 */
@Beta
public class ProgramClient {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(WorkflowActionSpecification.class, new WorkflowActionSpecificationCodec())
    .registerTypeAdapter(CustomActionSpecification.class, new CustomActionSpecificationCodec())
    .registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory())
    .create();
  private static final Type BATCH_STATUS_RESPONSE_TYPE = new TypeToken<List<BatchProgramStatus>>() { }.getType();
  private static final Type BATCH_RESULTS_TYPE = new TypeToken<List<BatchProgramResult>>() { }.getType();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final RESTClient restClient;
  private final ClientConfig config;
  private final ApplicationClient applicationClient;

  @Inject
  public ProgramClient(ClientConfig config, RESTClient restClient, ApplicationClient applicationClient) {
    this.config = config;
    this.restClient = restClient;
    this.applicationClient = applicationClient;
  }

  public ProgramClient(ClientConfig config) {
    this(config, new RESTClient(config));
  }

  public ProgramClient(ClientConfig config, RESTClient restClient) {
    this(config, restClient, new ApplicationClient(config, restClient));
  }

  /**
   * Starts a program using specified runtime arguments.
   *
   * @param program the program to start
   * @param debug true to start in debug mode
   * @param runtimeArgs runtime arguments to pass to the program
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the program with the specified name could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @deprecated since 4.0.0. Please use {@link #start(ProgramId, boolean, Map)} instead
   */
  @Deprecated
  public void start(Id.Program program, boolean debug, @Nullable Map<String, String> runtimeArgs)
    throws IOException, ProgramNotFoundException, UnauthenticatedException, UnauthorizedException {
    start(program.toEntityId(), debug, runtimeArgs);
  }

  /**
   * Starts a program using specified runtime arguments.
   *
   * @param program the program to start
   * @param debug true to start in debug mode
   * @param runtimeArgs runtime arguments to pass to the program
   * @throws IOException
   * @throws ProgramNotFoundException
   * @throws UnauthenticatedException
   * @throws UnauthorizedException
   */
  public void start(ProgramId program, boolean debug, @Nullable Map<String, String> runtimeArgs) throws IOException,
    ProgramNotFoundException, UnauthenticatedException, UnauthorizedException {
    String action = debug ? "debug" :  "start";
    String path = String.format("apps/%s/versions/%s/%s/%s/%s", program.getApplication(), program.getVersion(),
                                program.getType().getCategoryName(), program.getProgram(), action);
    URL url = config.resolveNamespacedURLV3(program.getNamespaceId(), path);
    HttpRequest.Builder request = HttpRequest.post(url);
    if (runtimeArgs != null) {
      request.withBody(GSON.toJson(runtimeArgs));
    }

    HttpResponse response = restClient.execute(request.build(), config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(program);
    }
  }

  /**
   * Starts a program using the stored runtime arguments.
   *
   * @param program the program to start
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the program with the specified name could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public void start(Id.Program program, boolean debug)
    throws IOException, ProgramNotFoundException, UnauthenticatedException, UnauthorizedException {

    start(program, debug, null);
  }

  /**
   * Starts a program using the stored runtime arguments.
   *
   * @param program the program to start
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the program with the specified name could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public void start(Id.Program program)
    throws IOException, ProgramNotFoundException, UnauthenticatedException, UnauthorizedException {
    start(program, false, null);
  }

  /**
   * Starts a batch of programs in the same call.
   *
   * @param namespace the namespace of the programs
   * @param programs the programs to start, including any runtime arguments to start them with
   * @return the result of starting each program
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public List<BatchProgramResult> start(Id.Namespace namespace, List<BatchProgramStart> programs)
    throws IOException, UnauthenticatedException, UnauthorizedException {

    URL url = config.resolveNamespacedURLV3(namespace, "start");
    HttpRequest request = HttpRequest.builder(HttpMethod.POST, url)
      .withBody(GSON.toJson(programs), Charsets.UTF_8).build();

    HttpResponse response = restClient.execute(request, config.getAccessToken());
    return ObjectResponse.<List<BatchProgramResult>>fromJsonBody(response, BATCH_RESULTS_TYPE, GSON)
      .getResponseObject();
  }

  /**
   * Stops a program.
   *
   * @param program the program to stop
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the program with the specified name could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @deprecated since 4.0.0. Please use {@link #stop(ProgramId)} instead
   */
  @Deprecated
  public void stop(Id.Program program)
    throws IOException, ProgramNotFoundException, UnauthenticatedException, UnauthorizedException {
    stop(program.toEntityId());
  }

  /**
   * Stops a program.
   *
   * @param programId the program to stop
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the program with specified name could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public void stop(ProgramId programId) throws IOException, ProgramNotFoundException, UnauthenticatedException,
    UnauthorizedException {
    String path = String.format("apps/%s/versions/%s/%s/%s/stop", programId.getApplication(), programId.getVersion(),
                                programId.getType().getCategoryName(), programId.getProgram());
    URL url = config.resolveNamespacedURLV3(programId.getNamespaceId().toId(), path);
    HttpResponse response = restClient.execute(HttpMethod.POST, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(programId);
    }
  }

  /**
   * Stops a batch of programs in the same call.
   *
   * @param namespace the namespace of the programs
   * @param programs the programs to stop
   * @return the result of stopping each program
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public List<BatchProgramResult> stop(Id.Namespace namespace, List<BatchProgram> programs)
    throws IOException, UnauthenticatedException, UnauthorizedException {

    URL url = config.resolveNamespacedURLV3(namespace, "stop");
    HttpRequest request = HttpRequest.builder(HttpMethod.POST, url)
      .withBody(GSON.toJson(programs), Charsets.UTF_8).build();

    HttpResponse response = restClient.execute(request, config.getAccessToken());
    return ObjectResponse.<List<BatchProgramResult>>fromJsonBody(response, BATCH_RESULTS_TYPE, GSON)
      .getResponseObject();
  }

  /**
   * Stops all currently running programs.
   *
   * @throws IOException
   * @throws UnauthenticatedException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  public void stopAll(Id.Namespace namespace)
    throws IOException, UnauthenticatedException, InterruptedException, TimeoutException, UnauthorizedException,
    ApplicationNotFoundException {

    List<ApplicationRecord> allApps = applicationClient.list(namespace);
    for (ApplicationRecord applicationRecord : allApps) {
      ApplicationId appId = new ApplicationId(namespace.getId(), applicationRecord.getName(),
                                              applicationRecord.getAppVersion());
      List<ProgramRecord> programRecords = applicationClient.listPrograms(appId);
      for (ProgramRecord programRecord : programRecords) {
        try {
          ProgramId program = appId.program(programRecord.getType(), programRecord.getName());
          String status = this.getStatus(program);
          if (!status.equals("STOPPED")) {
            this.stop(program);
            this.waitForStatus(program, ProgramStatus.STOPPED, 60, TimeUnit.SECONDS);
          }
        } catch (ProgramNotFoundException e) {
          // IGNORE
        }
      }
    }
  }

  /**
   * Gets the status of a program.
   *
   * @param program the program
   * @return the status of the program (e.g. STOPPED, STARTING, RUNNING)
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the program with the specified name could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @deprecated since 4.0.0. Please use {@link #getStatus(ProgramId)} instead
   */
  @Deprecated
  public String getStatus(Id.Program program)
    throws IOException, ProgramNotFoundException, UnauthenticatedException, UnauthorizedException {
    return getStatus(program.toEntityId());
  }

  /**
   * Gets the status of a program
   * @param programId the program
   * @return the status of the program (e.g. STOPPED, STARTING, RUNNING)
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the program with the specified name could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public String getStatus(ProgramId programId) throws IOException, ProgramNotFoundException, UnauthenticatedException,
    UnauthorizedException {
    String path = String.format("apps/%s/versions/%s/%s/%s/status", programId.getApplication(), programId.getVersion(),
                                programId.getType().getCategoryName(), programId.getProgram());
    URL url = config.resolveNamespacedURLV3(programId.getNamespaceId(), path);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new ProgramNotFoundException(programId);
    }

    Map<String, String> responseObject = ObjectResponse.<Map<String, String>>fromJsonBody(
      response, MAP_STRING_STRING_TYPE, GSON).getResponseObject();
    return responseObject.get("status");
  }

  /**
   * Gets the status of multiple programs.
   *
   * @param namespace the namespace of the programs
   * @param programs the list of programs to get status for
   * @return the status of each program
   */
  public List<BatchProgramStatus> getStatus(Id.Namespace namespace, List<BatchProgram> programs)
    throws IOException, UnauthenticatedException, UnauthorizedException {

    URL url = config.resolveNamespacedURLV3(namespace, "status");

    HttpRequest request = HttpRequest.post(url).withBody(GSON.toJson(programs)).build();
    HttpResponse response = restClient.execute(request, config.getAccessToken());

    return ObjectResponse.<List<BatchProgramStatus>>fromJsonBody(response, BATCH_STATUS_RESPONSE_TYPE, GSON)
      .getResponseObject();
  }

  /**
   * Waits for a program to have a certain status.
   *
   * @param program the program
   * @param status the desired status
   * @param timeout how long to wait in milliseconds until timing out
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the program with the specified name could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws TimeoutException if the program did not achieve the desired program status before the timeout
   * @throws InterruptedException if interrupted while waiting for the desired program status
   * @deprecated since 4.0.0. Please use {@link #waitForStatus(ProgramId, ProgramStatus, long, TimeUnit)} instead
   */
  @Deprecated
  public void waitForStatus(final Id.Program program, String status, long timeout, TimeUnit timeoutUnit)
    throws UnauthenticatedException, IOException, ProgramNotFoundException,
    TimeoutException, InterruptedException {
    waitForStatus(program.toEntityId(), ProgramStatus.valueOf(status), timeout, timeoutUnit);
  }

  /**
   * Waits for a program to have a certain status.
   *
   * @param program the program
   * @param status the desired status
   * @param timeout how long to wait in milliseconds until timing out
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the program with the specified name could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws TimeoutException if the program did not achieve the desired program status before the timeout
   * @throws InterruptedException if interrupted while waiting for the desired program status
   */
  public void waitForStatus(final ProgramId program, ProgramStatus status, long timeout, TimeUnit timeoutUnit)
    throws UnauthenticatedException, IOException, ProgramNotFoundException,
    TimeoutException, InterruptedException {

    try {
      Tasks.waitFor(status.name(), new Callable<String>() {
        @Override
        public String call() throws Exception {
          return getStatus(program);
        }
      }, timeout, timeoutUnit, 1, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), UnauthenticatedException.class);
      Throwables.propagateIfPossible(e.getCause(), ProgramNotFoundException.class);
      Throwables.propagateIfPossible(e.getCause(), IOException.class);
      Throwables.propagate(e.getCause());
    }
  }

  /**
   * Gets the live information of a program. In distributed CDAP,
   * this will contain IDs of the YARN applications that are running the program.
   *
   * @param program the program
   * @return {@link ProgramLiveInfo} of the program
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the program with the specified name could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public DistributedProgramLiveInfo getLiveInfo(Id.Program program)
    throws IOException, ProgramNotFoundException, UnauthenticatedException, UnauthorizedException {

    String path = String.format("apps/%s/%s/%s/live-info",
                                program.getApplicationId(), program.getType().getCategoryName(), program.getId());
    URL url = config.resolveNamespacedURLV3(program.getNamespace(), path);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(program.toEntityId());
    }

    return ObjectResponse.fromJsonBody(response, DistributedProgramLiveInfo.class).getResponseObject();
  }

  /**
   * Gets the number of instances that a flowlet is currently running on.
   *
   * @param flowlet the flowlet
   * @return number of instances that the flowlet is currently running on
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application, flow, or flowlet could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public int getFlowletInstances(Id.Flow.Flowlet flowlet)
    throws IOException, NotFoundException, UnauthenticatedException, UnauthorizedException {

    URL url = config.resolveNamespacedURLV3(flowlet.getNamespace(),
                                            String.format("apps/%s/flows/%s/flowlets/%s/instances",
                                                          flowlet.getFlow().getApplicationId(),
                                                          flowlet.getFlow().getId(),
                                                          flowlet.getId()));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(flowlet.toEntityId());
    }

    return ObjectResponse.fromJsonBody(response, Instances.class).getResponseObject().getInstances();
  }

  /**
   * Sets the number of instances that a flowlet will run on.
   *
   * @param flowlet the flowlet
   * @param instances number of instances for the flowlet to run on
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application, flow, or flowlet could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public void setFlowletInstances(Id.Flow.Flowlet flowlet, int instances)
    throws IOException, NotFoundException, UnauthenticatedException, UnauthorizedException {

    URL url = config.resolveNamespacedURLV3(flowlet.getNamespace(),
                                            String.format("apps/%s/flows/%s/flowlets/%s/instances",
                                                          flowlet.getFlow().getApplicationId(),
                                                          flowlet.getFlow().getId(),
                                                          flowlet.getId()));
    HttpRequest request = HttpRequest.put(url).withBody(GSON.toJson(new Instances(instances))).build();

    HttpResponse response = restClient.execute(request, config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(flowlet.toEntityId());
    }
  }

  /**
   * Gets the number of instances that a worker is currently running on.
   *
   * @param worker the worker
   * @return number of instances that the worker is currently running on
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application or worker could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public int getWorkerInstances(Id.Worker worker) throws IOException, NotFoundException,
    UnauthenticatedException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3(worker.getNamespace(),
                                            String.format("apps/%s/workers/%s/instances",
                                                          worker.getApplicationId(), worker.getId()));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(worker.toEntityId());
    }
    return ObjectResponse.fromJsonBody(response, Instances.class).getResponseObject().getInstances();
  }

  /**
   * Sets the number of instances that a worker will run on.
   *
   * @param instances number of instances for the worker to run on
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application or worker could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public void setWorkerInstances(Id.Worker worker, int instances) throws IOException, NotFoundException,
    UnauthenticatedException, UnauthorizedException {

    URL url = config.resolveNamespacedURLV3(worker.getNamespace(),
                                            String.format("apps/%s/workers/%s/instances",
                                                          worker.getApplicationId(), worker.getId()));
    HttpRequest request = HttpRequest.put(url).withBody(GSON.toJson(new Instances(instances))).build();

    HttpResponse response = restClient.execute(request, config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(worker.toEntityId());
    }
  }

  /**
   * Gets the number of instances of a service.
   *
   * @param service the service
   * @return number of instances of the service handler
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application or service could not found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public int getServiceInstances(Id.Service service)
    throws IOException, NotFoundException, UnauthenticatedException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3(service.getNamespace(),
                                            String.format("apps/%s/services/%s/instances",
                                                          service.getApplicationId(),
                                                          service.getId()));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(service.toEntityId());
    }
    return ObjectResponse.fromJsonBody(response, Instances.class).getResponseObject().getInstances();
  }

  /**
   * Sets the number of instances of a service.
   *
   * @param service the service
   * @param instances number of instances for the service
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application or service could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public void setServiceInstances(Id.Service service, int instances)
    throws IOException, NotFoundException, UnauthenticatedException, UnauthorizedException {

    URL url = config.resolveNamespacedURLV3(service.getNamespace(),
                                            String.format("apps/%s/services/%s/instances",
                                                          service.getApplicationId(),
                                                          service.getId()));
    HttpRequest request = HttpRequest.put(url).withBody(GSON.toJson(new Instances(instances))).build();
    HttpResponse response = restClient.execute(request, config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(service.toEntityId());
    }
  }

  /**
   * Get the current run information for the Workflow based on the runid
   * @param appId ID of the application
   * @param workflowId ID of the workflow
   * @param runId ID of the run for which the details are to be returned
   * @return list of {@link WorkflowActionNode} currently running for the given runid
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application, workflow, or runid could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public List<WorkflowActionNode> getWorkflowCurrent(Id.Application appId, String workflowId, String runId)
    throws IOException, NotFoundException, UnauthenticatedException, UnauthorizedException {
    String path = String.format("/apps/%s/workflows/%s/runs/%s/current", appId.getId(), workflowId, runId);
    URL url = config.resolveNamespacedURLV3(appId.getNamespace(), path);

    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(appId.toEntityId().workflow(workflowId).run(runId));
    }

    ObjectResponse<List<WorkflowActionNode>> objectResponse = ObjectResponse.fromJsonBody(
      response, new TypeToken<List<WorkflowActionNode>>() { }.getType(), GSON);

    return objectResponse.getResponseObject();
  }

  /**
   * Gets the run records of a program.
   *
   * @param program the program
   * @param state - filter by status of the program
   * @return the run records of the program
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application or program could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public List<RunRecord> getProgramRuns(Id.Program program, String state,
                                        long startTime, long endTime, int limit)
    throws IOException, NotFoundException, UnauthenticatedException, UnauthorizedException {

    String queryParams = String.format("%s=%s&%s=%d&%s=%d&%s=%d",
                                       Constants.AppFabric.QUERY_PARAM_STATUS, state,
                                       Constants.AppFabric.QUERY_PARAM_START_TIME, startTime,
                                       Constants.AppFabric.QUERY_PARAM_END_TIME, endTime,
                                       Constants.AppFabric.QUERY_PARAM_LIMIT, limit);

    String path = String.format("apps/%s/%s/%s/runs?%s",
                                program.getApplicationId(),
                                program.getType().getCategoryName(),
                                program.getId(), queryParams);
    URL url = config.resolveNamespacedURLV3(program.getNamespace(), path);

    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(program.toEntityId());
    }

    return ObjectResponse.fromJsonBody(response, new TypeToken<List<RunRecord>>() { }).getResponseObject();
  }

  /**
   * Gets the run records of a program.
   *
   * @param program ID of the program
   * @return the run records of the program
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application or program could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public List<RunRecord> getAllProgramRuns(Id.Program program, long startTime, long endTime, int limit)
    throws IOException, NotFoundException, UnauthenticatedException, UnauthorizedException {
    return getProgramRuns(program, ProgramRunStatus.ALL.name(), startTime, endTime, limit);
  }


  /**
   * Gets the logs of a program.
   *
   * @param program the program
   * @param start start time of the time range of desired logs
   * @param stop end time of the time range of desired logs
   * @return the logs of the program
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application or program could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public String getProgramLogs(Id.Program program, long start, long stop)
    throws IOException, NotFoundException, UnauthenticatedException, UnauthorizedException {

    String path = String.format("apps/%s/%s/%s/logs?start=%d&stop=%d&escape=false",
                                program.getApplicationId(), program.getType().getCategoryName(),
                                program.getId(), start, stop);
    URL url = config.resolveNamespacedURLV3(program.getNamespace(), path);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken());
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(program.toEntityId());
    }

    return new String(response.getResponseBody(), Charsets.UTF_8);
  }

  /**
   * Gets the runtime args of a program.
   *
   * @param program the program
   * @return runtime args of the program
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the application or program could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @deprecated since 4.0.0. Please use {@link #getRuntimeArgs(ProgramId)} instead
   */
  @Deprecated
  public Map<String, String> getRuntimeArgs(Id.Program program)
    throws IOException, UnauthenticatedException, ProgramNotFoundException, UnauthorizedException {

    return getRuntimeArgs(program.toEntityId());
  }

  /**
   * Gets the runtime args of a program.
   *
   * @param program the program
   * @return runtime args of the program
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the application or program could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public Map<String, String> getRuntimeArgs(ProgramId program)
    throws IOException, UnauthenticatedException, ProgramNotFoundException, UnauthorizedException {

    String path = String.format("apps/%s/versions/%s/%s/%s/runtimeargs",
                                program.getApplication(), program.getVersion(),
                                program.getType().getCategoryName(), program.getProgram());
    URL url = config.resolveNamespacedURLV3(program.getNamespaceId(), path);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(program);
    }
    return ObjectResponse.fromJsonBody(response, new TypeToken<Map<String, String>>() { }).getResponseObject();
  }

  /**
   * Sets the runtime args of a program.
   *
   * @param program the program
   * @param runtimeArgs args of the program
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the application or program could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @deprecated since 4.0.0. Please use {@link #setRuntimeArgs(ProgramId, Map)} instead
   */
  @Deprecated
  public void setRuntimeArgs(Id.Program program, Map<String, String> runtimeArgs)
    throws IOException, UnauthenticatedException, ProgramNotFoundException, UnauthorizedException {

    setRuntimeArgs(program.toEntityId(), runtimeArgs);
  }

  /**
   * Sets the runtime args of a program.
   *
   * @param program the program
   * @param runtimeArgs args of the program
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the application or program could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public void setRuntimeArgs(ProgramId program, Map<String, String> runtimeArgs)
    throws IOException, UnauthenticatedException, ProgramNotFoundException, UnauthorizedException {

    String path = String.format("apps/%s/versions/%s/%s/%s/runtimeargs",
                                program.getApplication(), program.getVersion(),
                                program.getType().getCategoryName(), program.getProgram());
    URL url = config.resolveNamespacedURLV3(program.getNamespaceId(), path);
    HttpRequest request = HttpRequest.put(url).withBody(GSON.toJson(runtimeArgs)).build();
    HttpResponse response = restClient.execute(request, config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(program);
    }
  }
}
