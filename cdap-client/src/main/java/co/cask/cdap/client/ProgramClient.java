/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.DistributedProgramLiveInfo;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.Instances;
import co.cask.cdap.proto.ProgramLiveInfo;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.codec.WorkflowActionSpecificationCodec;
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
    .create();

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
   * @param appId ID of the application that the program belongs to
   * @param programType type of the program
   * @param programName name of the program
   * @param debug true to start in debug mode
   * @param runtimeArgs runtime arguments to pass to the program
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the program with the specified name could not be found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   *
   * @deprecated As of 3.1, use {@link #start(Id.Program, boolean, Map)} instead.
   */
  @Deprecated
  public void start(String appId, ProgramType programType, String programName,
                    boolean debug, @Nullable Map<String, String> runtimeArgs)
    throws IOException, ProgramNotFoundException, UnauthorizedException {

    start(Id.Program.from(config.getNamespace(), appId, programType, programName), debug, runtimeArgs);
  }

  /**
   * Starts a program, giving debug mode, using the stored runtime arguments.
   *
   * @param appId ID of the application that the program belongs to
   * @param programType type of the program
   * @param programName name of the program
   * @param debug true to start in debug mode
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the program with the specified name could not be found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   *
   * @deprecated As of 3.1, use {@link #start(Id.Program, boolean)} instead.
   */
  @Deprecated
  public void start(String appId, ProgramType programType, String programName, boolean debug)
    throws IOException, ProgramNotFoundException, UnauthorizedException {

    start(Id.Program.from(config.getNamespace(), appId, programType, programName), debug);
  }

  /**
   * Starts a program using the stored runtime arguments.
   *
   * @param appId ID of the application that the program belongs to
   * @param programType type of the program
   * @param programName name of the program
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the program with the specified name could not be found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   *
   * @deprecated As of 3.1, use {@link #start(Id.Program)} instead.
   */
  @Deprecated
  public void start(String appId, ProgramType programType, String programName)
    throws IOException, ProgramNotFoundException, UnauthorizedException {

    start(Id.Program.from(config.getNamespace(), appId, programType, programName));
  }

  /**
   * Stops a program.
   *
   * @param appId ID of the application that the program belongs to
   * @param programType type of the program
   * @param programName name of the program
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the program with the specified name could not be found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   *
   * @deprecated As of 3.1, use {@link #stop(Id.Program)} instead.
   */
  @Deprecated
  public void stop(String appId, ProgramType programType, String programName)
    throws IOException, ProgramNotFoundException, UnauthorizedException {

    stop(Id.Program.from(config.getNamespace(), appId, programType, programName));
  }

  /**
   * Stops all currently running programs.
   *
   * @throws IOException
   * @throws UnauthorizedException
   * @throws InterruptedException
   * @throws TimeoutException
   *
   * @deprecated As of 3.1, use {@link #stopAll(Id.Namespace)} instead.
   */
  @Deprecated
  public void stopAll() throws IOException, UnauthorizedException, InterruptedException, TimeoutException {
    stopAll(config.getNamespace());
  }

  /**
   * Gets the status of a program.
   *
   * @param appId ID of the application that the program belongs to
   * @param programType type of the program
   * @param programName name of the program
   * @return the status of the program (e.g. STOPPED, STARTING, RUNNING)
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the program with the specified name could not be found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   *
   * @deprecated As of 3.1, use {@link #getStatus(Id.Program)} instead.
   */
  @Deprecated
  public String getStatus(String appId, ProgramType programType, String programName)
    throws IOException, ProgramNotFoundException, UnauthorizedException {

    return getStatus(Id.Program.from(config.getNamespace(), appId, programType, programName));
  }

  /**
   * Waits for a program to have a certain status.
   *
   * @param appId ID of the application that the program belongs to
   * @param programType type of the program
   * @param programName name of the program
   * @param status the desired status
   * @param timeout how long to wait in milliseconds until timing out
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the program with the specified name could not be found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws TimeoutException if the program did not achieve the desired program status before the timeout
   * @throws InterruptedException if interrupted while waiting for the desired program status
   *
   * @deprecated As of 3.1, use {@link #waitForStatus(Id.Program, String, long, TimeUnit)} instead.
   */
  @Deprecated
  public void waitForStatus(final String appId, final ProgramType programType, final String programName,
                            String status, long timeout, TimeUnit timeoutUnit)
    throws UnauthorizedException, IOException, ProgramNotFoundException,
    TimeoutException, InterruptedException {

    waitForStatus(Id.Program.from(config.getNamespace(), appId, programType, programName),
                  status, timeout, timeoutUnit);
  }

  /**
   * Gets the live information of a program. In distributed CDAP,
   * this will contain IDs of the YARN applications that are running the program.
   *
   * @param appId ID of the application that the program belongs to
   * @param programType type of the program
   * @param programName name of the program
   * @return {@link ProgramLiveInfo} of the program
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the program with the specified name could not be found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   *
   * @deprecated As of 3.1, use {@link #getLiveInfo(Id.Program)} instead.
   */
  @Deprecated
  public DistributedProgramLiveInfo getLiveInfo(String appId, ProgramType programType, String programName)
    throws IOException, ProgramNotFoundException, UnauthorizedException {

    return getLiveInfo(Id.Program.from(config.getNamespace(), appId, programType, programName));
  }

  /**
   * Gets the number of instances that a flowlet is currently running on.
   *
   * @param appId ID of the application that the flowlet belongs to
   * @param flowId ID of the flow that the flowlet belongs to
   * @param flowletId ID of the flowlet
   * @return number of instances that the flowlet is currently running on
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application, flow, or flowlet could not be found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   *
   * @deprecated As of 3.1, use {@link #getFlowletInstances(Id.Flow.Flowlet)} instead.
   */
  @Deprecated
  public int getFlowletInstances(String appId, String flowId, String flowletId)
    throws IOException, NotFoundException, UnauthorizedException {

    return getFlowletInstances(Id.Flow.Flowlet.from(Id.Flow.from(config.getNamespace(), appId, flowId), flowletId));
  }

  /**
   * Sets the number of instances that a flowlet will run on.
   *
   * @param appId ID of the application that the flowlet belongs to
   * @param flowId ID of the flow that the flowlet belongs to
   * @param flowletId ID of the flowlet
   * @param instances number of instances for the flowlet to run on
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application, flow, or flowlet could not be found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   *
   * @deprecated As of 3.1, use {@link #setFlowletInstances(Id.Flow.Flowlet, int)} instead.
   */
  @Deprecated
  public void setFlowletInstances(String appId, String flowId, String flowletId, int instances)
    throws IOException, NotFoundException, UnauthorizedException {

    setFlowletInstances(Id.Flow.Flowlet.from(Id.Flow.from(config.getNamespace(), appId, flowId), flowletId),
                        instances);
  }

  /**
   * Gets the number of instances that a worker is currently running on.
   *
   * @param appId ID of the application that the worker belongs to
   * @param workerId ID of the worker
   * @return number of instances that the worker is currently running on
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application or worker could not be found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   *
   * @deprecated As of 3.1, use {@link #getWorkerInstances(Id.Worker)} instead.
   */
  @Deprecated
  public int getWorkerInstances(String appId, String workerId) throws IOException, NotFoundException,
    UnauthorizedException {

    return getWorkerInstances(Id.Worker.from(config.getNamespace(), appId, workerId));
  }

  /**
   * Sets the number of instances that a worker will run on.
   *
   * @param appId ID of the application that the worker belongs to
   * @param workerId ID of the worker
   * @param instances number of instances for the worker to run on
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application or worker could not be found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   *
   * @deprecated As of 3.1, use {@link #setWorkerInstances(Id.Worker, int)} instead.
   */
  @Deprecated
  public void setWorkerInstances(String appId, String workerId, int instances) throws IOException, NotFoundException,
    UnauthorizedException {

    setWorkerInstances(Id.Worker.from(config.getNamespace(), appId, workerId), instances);
  }

  /**
   * Gets the number of instances of a service.
   *
   * @param appId ID of the application that the service belongs to
   * @param serviceId Id of the service
   * @return number of instances of the service handler
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application or service could not found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   *
   * @deprecated As of 3.1, use {@link #getServiceInstances(Id.Service)} instead.
   */
  @Deprecated
  public int getServiceInstances(String appId, String serviceId)
    throws IOException, NotFoundException, UnauthorizedException {

    return getServiceInstances(Id.Service.from(config.getNamespace(), appId, serviceId));
  }

  /**
   * Sets the number of instances of a service.
   *
   * @param appId ID of the application that the service belongs to
   * @param serviceId ID of the service
   * @param instances number of instances for the service
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application or service could not be found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   *
   * @deprecated As of 3.1, use {@link #setServiceInstances(Id.Service, int)} instead.
   */
  @Deprecated
  public void setServiceInstances(String appId, String serviceId, int instances)
    throws IOException, NotFoundException, UnauthorizedException {

    setServiceInstances(Id.Service.from(config.getNamespace(), appId, serviceId), instances);
  }

  /**
   * Get the current run information for the Workflow based on the runid
   * @param appId ID of the application
   * @param workflowId ID of the workflow
   * @param runId ID of the run for which the details are to be returned
   * @return list of {@link WorkflowActionNode} currently running for the given runid
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application, workflow, or runid could not be found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   *
   * @deprecated As of 3.1, use {@link #getWorkflowCurrent(Id.Application, String, String)} instead.
   */
  @Deprecated
  public List<WorkflowActionNode> getWorkflowCurrent(String appId, String workflowId, String runId)
    throws IOException, NotFoundException, UnauthorizedException {

    return getWorkflowCurrent(Id.Application.from(config.getNamespace(), appId), workflowId, runId);
  }

  /**
   * Gets the run records of a program.
   *
   * @param appId ID of the application that the program belongs to
   * @param programType type of the program
   * @param programId ID of the program
   * @param state - filter by status of the program
   * @return the run records of the program
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application or program could not be found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   *
   * @deprecated As of 3.1, use {@link #getProgramRuns(Id.Program, String, long, long, int)} instead.
   */
  @Deprecated
  public List<RunRecord> getProgramRuns(String appId, ProgramType programType, String programId, String state,
                                        long startTime, long endTime, int limit)
    throws IOException, NotFoundException, UnauthorizedException {

    return getProgramRuns(Id.Program.from(config.getNamespace(), appId, programType, programId),
                          state, startTime, endTime, limit);
  }

  /**
   * Gets the run records of a program.
   *
   * @param appId ID of the application that the program belongs to
   * @param programType type of the program
   * @param programId ID of the program
   * @return the run records of the program
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application or program could not be found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   *
   * @deprecated As of 3.1, use {@link #getAllProgramRuns(Id.Program, long, long, int)} instead.
   */
  @Deprecated
  public List<RunRecord> getAllProgramRuns(String appId, ProgramType programType, String programId,
                                           long startTime, long endTime, int limit)
    throws IOException, NotFoundException, UnauthorizedException {

    return getAllProgramRuns(Id.Program.from(config.getNamespace(), appId, programType, programId),
                             startTime, endTime, limit);
  }

  /**
   * Gets the logs of a program.
   *
   * @param appId ID of the application that the program belongs to
   * @param programType type of the program
   * @param programId ID of the program
   * @param start start time of the time range of desired logs
   * @param stop end time of the time range of desired logs
   * @return the logs of the program
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application or program could not be found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   *
   * @deprecated As of 3.1, use {@link #getProgramLogs(Id.Program, long, long)} instead.
   */
  @Deprecated
  public String getProgramLogs(String appId, ProgramType programType, String programId, long start, long stop)
    throws IOException, NotFoundException, UnauthorizedException {

    return getProgramLogs(Id.Program.from(config.getNamespace(), appId, programType, programId), start, stop);
  }

  /**
   * Gets the runtime args of a program.
   *
   * @param appId ID of the application that the program belongs to
   * @param programType type of the program
   * @param programId ID of the program
   * @return runtime args of the program
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the application or program could not be found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   *
   * @deprecated As of 3.1, use {@link #getRuntimeArgs(Id.Program)} instead.
   */
  @Deprecated
  public Map<String, String> getRuntimeArgs(String appId, ProgramType programType, String programId)
    throws IOException, UnauthorizedException, ProgramNotFoundException {

    return getRuntimeArgs(Id.Program.from(config.getNamespace(), appId, programType, programId));
  }

  /**
   * Sets the runtime args of a program.
   *
   * @param appId ID of the application that the program belongs to
   * @param programType type of the program
   * @param programId ID of the program
   * @param runtimeArgs args of the program
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the application or program could not be found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   *
   * @deprecated As of 3.1, use {@link #setRuntimeArgs(Id.Program, Map)} instead.
   */
  @Deprecated
  public void setRuntimeArgs(String appId, ProgramType programType, String programId, Map<String, String> runtimeArgs)
    throws IOException, UnauthorizedException, ProgramNotFoundException {

    setRuntimeArgs(Id.Program.from(config.getNamespace(), appId, programType, programId), runtimeArgs);
  }

  /**
   * Starts a program using specified runtime arguments.
   *
   * @param program the program to start
   * @param debug true to start in debug mode
   * @param runtimeArgs runtime arguments to pass to the program
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the program with the specified name could not be found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public void start(Id.Program program, boolean debug, @Nullable Map<String, String> runtimeArgs)
    throws IOException, ProgramNotFoundException, UnauthorizedException {

    String action = debug ? "debug" : "start";
    String path = String.format("apps/%s/%s/%s/%s",
                                program.getApplicationId(),
                                program.getType().getCategoryName(),
                                program.getId(), action);
    URL url = config.resolveNamespacedURLV3(program.getNamespace(), path);
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
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public void start(Id.Program program, boolean debug)
    throws IOException, ProgramNotFoundException, UnauthorizedException {

    start(program, debug, null);
  }

  /**
   * Starts a program using the stored runtime arguments.
   *
   * @param program the program to start
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the program with the specified name could not be found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public void start(Id.Program program) throws IOException, ProgramNotFoundException, UnauthorizedException {
    start(program, false, null);
  }

  /**
   * Stops a program.
   *
   * @param program the program to stop
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the program with the specified name could not be found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public void stop(Id.Program program) throws IOException, ProgramNotFoundException, UnauthorizedException {
    String path = String.format("apps/%s/%s/%s/stop",
                                program.getApplicationId(), program.getType().getCategoryName(), program.getId());
    URL url = config.resolveNamespacedURLV3(program.getNamespace(), path);
    HttpResponse response = restClient.execute(HttpMethod.POST, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(program);
    }
  }

  /**
   * Stops all currently running programs.
   *
   * @throws IOException
   * @throws UnauthorizedException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  public void stopAll(Id.Namespace namespace)
    throws IOException, UnauthorizedException, InterruptedException, TimeoutException {

    Map<ProgramType, List<ProgramRecord>> allPrograms = applicationClient.listAllPrograms(namespace);
    for (Map.Entry<ProgramType, List<ProgramRecord>> entry : allPrograms.entrySet()) {
      ProgramType programType = entry.getKey();
      List<ProgramRecord> programRecords = entry.getValue();
      for (ProgramRecord programRecord : programRecords) {
        try {
          Id.Program program = Id.Program.from(namespace, programRecord.getApp(),
                                               programType, programRecord.getName());
          String status = this.getStatus(program);
          if (!status.equals("STOPPED")) {
            this.stop(program);
            this.waitForStatus(program, "STOPPED", 60, TimeUnit.SECONDS);
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
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public String getStatus(Id.Program program)
    throws IOException, ProgramNotFoundException, UnauthorizedException {

    String path = String.format("apps/%s/%s/%s/status",
                                program.getApplicationId(), program.getType().getCategoryName(), program.getId());
    URL url = config.resolveNamespacedURLV3(program.getNamespace(), path);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new ProgramNotFoundException(program);
    }

    return ObjectResponse.fromJsonBody(response, ProgramStatus.class).getResponseObject().getStatus();
  }

  /**
   * Waits for a program to have a certain status.
   *
   * @param program the program
   * @param status the desired status
   * @param timeout how long to wait in milliseconds until timing out
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the program with the specified name could not be found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   * @throws TimeoutException if the program did not achieve the desired program status before the timeout
   * @throws InterruptedException if interrupted while waiting for the desired program status
   */
  public void waitForStatus(final Id.Program program, String status, long timeout, TimeUnit timeoutUnit)
    throws UnauthorizedException, IOException, ProgramNotFoundException,
    TimeoutException, InterruptedException {

    try {
      Tasks.waitFor(status, new Callable<String>() {
        @Override
        public String call() throws Exception {
          return getStatus(program);
        }
      }, timeout, timeoutUnit, 1, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), UnauthorizedException.class);
      Throwables.propagateIfPossible(e.getCause(), ProgramNotFoundException.class);
      Throwables.propagateIfPossible(e.getCause(), IOException.class);
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
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public DistributedProgramLiveInfo getLiveInfo(Id.Program program)
    throws IOException, ProgramNotFoundException, UnauthorizedException {

    String path = String.format("apps/%s/%s/%s/live-info",
                                program.getApplicationId(), program.getType().getCategoryName(), program.getId());
    URL url = config.resolveNamespacedURLV3(program.getNamespace(), path);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(program);
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
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public int getFlowletInstances(Id.Flow.Flowlet flowlet)
    throws IOException, NotFoundException, UnauthorizedException {

    URL url = config.resolveNamespacedURLV3(flowlet.getNamespace(),
                                            String.format("apps/%s/flows/%s/flowlets/%s/instances",
                                                          flowlet.getFlow().getApplicationId(),
                                                          flowlet.getFlow().getId(),
                                                          flowlet.getId()));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(flowlet);
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
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public void setFlowletInstances(Id.Flow.Flowlet flowlet, int instances)
    throws IOException, NotFoundException, UnauthorizedException {

    URL url = config.resolveNamespacedURLV3(flowlet.getNamespace(),
                                            String.format("apps/%s/flows/%s/flowlets/%s/instances",
                                                          flowlet.getFlow().getApplicationId(),
                                                          flowlet.getFlow().getId(),
                                                          flowlet.getId()));
    HttpRequest request = HttpRequest.put(url).withBody(GSON.toJson(new Instances(instances))).build();

    HttpResponse response = restClient.execute(request, config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(flowlet);
    }
  }

  /**
   * Gets the number of instances that a worker is currently running on.
   *
   * @param worker the worker
   * @return number of instances that the worker is currently running on
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application or worker could not be found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public int getWorkerInstances(Id.Worker worker) throws IOException, NotFoundException,
    UnauthorizedException {
    URL url = config.resolveNamespacedURLV3(worker.getNamespace(),
                                            String.format("apps/%s/workers/%s/instances",
                                                          worker.getApplicationId(), worker.getId()));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(worker);
    }
    return ObjectResponse.fromJsonBody(response, Instances.class).getResponseObject().getInstances();
  }

  /**
   * Sets the number of instances that a worker will run on.
   *
   * @param instances number of instances for the worker to run on
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application or worker could not be found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public void setWorkerInstances(Id.Worker worker, int instances) throws IOException, NotFoundException,
    UnauthorizedException {

    URL url = config.resolveNamespacedURLV3(worker.getNamespace(),
                                            String.format("apps/%s/workers/%s/instances",
                                                          worker.getApplicationId(), worker.getId()));
    HttpRequest request = HttpRequest.put(url).withBody(GSON.toJson(new Instances(instances))).build();

    HttpResponse response = restClient.execute(request, config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(worker);
    }
  }

  /**
   * Gets the number of instances of a service.
   *
   * @param service the service
   * @return number of instances of the service handler
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application or service could not found
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public int getServiceInstances(Id.Service service) throws IOException, NotFoundException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3(service.getNamespace(),
                                            String.format("apps/%s/services/%s/instances",
                                                          service.getApplicationId(),
                                                          service.getId()));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(service);
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
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public void setServiceInstances(Id.Service service, int instances)
    throws IOException, NotFoundException, UnauthorizedException {

    URL url = config.resolveNamespacedURLV3(service.getNamespace(),
                                            String.format("apps/%s/services/%s/instances",
                                                          service.getApplicationId(),
                                                          service.getId()));
    HttpRequest request = HttpRequest.put(url).withBody(GSON.toJson(new Instances(instances))).build();
    HttpResponse response = restClient.execute(request, config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(service);
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
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public List<WorkflowActionNode> getWorkflowCurrent(Id.Application appId, String workflowId, String runId)
    throws IOException, NotFoundException, UnauthorizedException {
    String path = String.format("/apps/%s/workflows/%s/runs/%s/current", appId.getId(), workflowId, runId);
    URL url = config.resolveNamespacedURLV3(appId.getNamespace(), path);

    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      Id.Program program = Id.Program.from(appId, ProgramType.WORKFLOW, workflowId);
      throw new NotFoundException(new Id.Run(program, runId));
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
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public List<RunRecord> getProgramRuns(Id.Program program, String state,
                                        long startTime, long endTime, int limit)
    throws IOException, NotFoundException, UnauthorizedException {

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
      throw new NotFoundException(program);
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
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public List<RunRecord> getAllProgramRuns(Id.Program program, long startTime, long endTime, int limit)
    throws IOException, NotFoundException, UnauthorizedException {
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
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public String getProgramLogs(Id.Program program, long start, long stop)
    throws IOException, NotFoundException, UnauthorizedException {

    String path = String.format("apps/%s/%s/%s/logs?start=%d&stop=%d",
                                program.getApplicationId(), program.getType().getCategoryName(),
                                program.getId(), start, stop);
    URL url = config.resolveNamespacedURLV3(program.getNamespace(), path);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken());
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(program);
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
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public Map<String, String> getRuntimeArgs(Id.Program program)
    throws IOException, UnauthorizedException, ProgramNotFoundException {

    String path = String.format("apps/%s/%s/%s/runtimeargs",
                                program.getApplicationId(), program.getType().getCategoryName(), program.getId());
    URL url = config.resolveNamespacedURLV3(program.getNamespace(), path);
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
   * @throws UnauthorizedException if the request is not authorized successfully in the gateway server
   */
  public void setRuntimeArgs(Id.Program program, Map<String, String> runtimeArgs)
    throws IOException, UnauthorizedException, ProgramNotFoundException {

    String path = String.format("apps/%s/%s/%s/runtimeargs",
                                program.getApplicationId(), program.getType().getCategoryName(), program.getId());
    URL url = config.resolveNamespacedURLV3(program.getNamespace(), path);
    HttpRequest request = HttpRequest.put(url).withBody(GSON.toJson(runtimeArgs)).build();
    HttpResponse response = restClient.execute(request, config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(program);
    }
  }
}
