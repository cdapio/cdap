/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.client;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.customaction.CustomActionSpecification;
import io.cdap.cdap.api.workflow.ConditionSpecification;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.util.RESTClient;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.ProgramNotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.BatchProgram;
import io.cdap.cdap.proto.BatchProgramResult;
import io.cdap.cdap.proto.BatchProgramStart;
import io.cdap.cdap.proto.BatchProgramStatus;
import io.cdap.cdap.proto.DistributedProgramLiveInfo;
import io.cdap.cdap.proto.Instances;
import io.cdap.cdap.proto.ProgramLiveInfo;
import io.cdap.cdap.proto.ProgramRecord;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramStatus;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.codec.ConditionSpecificationCodec;
import io.cdap.cdap.proto.codec.CustomActionSpecificationCodec;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ServiceId;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.cdap.common.http.ObjectResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG = LoggerFactory.getLogger(ProgramClient.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(CustomActionSpecification.class, new CustomActionSpecificationCodec())
    .registerTypeAdapter(ConditionSpecification.class, new ConditionSpecificationCodec())
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
    //Required to add body even if runtimeArgs is null to avoid 411 error for Http POST
    HttpRequest.Builder request = HttpRequest.post(url).withBody(GSON.toJson(runtimeArgs));;
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
  public void start(ProgramId program, boolean debug)
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
  public void start(ProgramId program)
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
  public List<BatchProgramResult> start(NamespaceId namespace, List<BatchProgramStart> programs)
    throws IOException, UnauthenticatedException, UnauthorizedException {

    URL url = config.resolveNamespacedURLV3(namespace, "start");
    HttpRequest request = HttpRequest.builder(HttpMethod.POST, url)
      .withBody(GSON.toJson(programs), Charsets.UTF_8).build();

    HttpResponse response = restClient.execute(request, config.getAccessToken());
    return ObjectResponse.<List<BatchProgramResult>>fromJsonBody(response, BATCH_RESULTS_TYPE, GSON)
      .getResponseObject();
  }

  /**
   * Restarts programs stopped between startTimeSeconds and endTimeSeconds in the application.
   *
   * @param applicationId to restart programs in
   * @param startTimeSeconds the lower bound of when programs were stopped to restart (inclusive)
   * @param endTimeSeconds the upper bound of when programs were stopped to restart (exclusive)
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authenticated successfully in the gateway server
   * @throws NotFoundException if the application cannot be found
   * @throws UnauthorizedException if the request is not authorized successfully
   */
  public void restart(ApplicationId applicationId, long startTimeSeconds, long endTimeSeconds)
    throws IOException, UnauthenticatedException, UnauthorizedException, NotFoundException {
    String path = String.format("apps/%s/versions/%s/restart-programs?start-time-seconds=%d&end-time-seconds=%d",
                                applicationId.getApplication(), applicationId.getVersion(), startTimeSeconds,
                                endTimeSeconds);
    URL url = config.resolveNamespacedURLV3(applicationId.getNamespaceId(), path);
    HttpResponse response = restClient.execute(HttpMethod.PUT, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException(applicationId);
    }
  }

  /**
   * Stops a program.
   *
   * @param programId the program to stop
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the program with specified name could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   * @throws BadRequestException if an attempt is made to stop a program that is either not running or
   *                             was started by a workflow
   */
  public void stop(ProgramId programId)
      throws IOException, ProgramNotFoundException, UnauthenticatedException,
      UnauthorizedException, BadRequestException {
    String path = String.format("apps/%s/versions/%s/%s/%s/stop", programId.getApplication(), programId.getVersion(),
                                programId.getType().getCategoryName(), programId.getProgram());
    URL url = config.resolveNamespacedURLV3(programId.getNamespaceId(), path);
    //Required to add body even if runtimeArgs is null to avoid 411 error for Http POST
    HttpRequest.Builder request = HttpRequest.post(url).withBody("");
    HttpResponse response = restClient.execute(request.build(), config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND, HttpURLConnection.HTTP_BAD_REQUEST);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(programId);
    }

    if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException(response.getResponseCode() + ": " + response.getResponseBodyAsString());
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
  public List<BatchProgramResult> stop(NamespaceId namespace, List<BatchProgram> programs)
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
   */
  public void stopAll(NamespaceId namespace)
    throws IOException, UnauthenticatedException, InterruptedException, TimeoutException, UnauthorizedException,
    ApplicationNotFoundException, BadRequestException {

    List<ApplicationRecord> allApps = applicationClient.list(namespace);
    for (ApplicationRecord applicationRecord : allApps) {
      ApplicationId appId = new ApplicationId(namespace.getNamespace(), applicationRecord.getName(),
                                              applicationRecord.getAppVersion());
      List<ProgramRecord> programRecords = applicationClient.listPrograms(appId);
      for (ProgramRecord programRecord : programRecords) {
        try {
          ProgramId program = appId.program(programRecord.getType(), programRecord.getName());
          String status = this.getStatus(program);
          if (!status.equals("STOPPED")) {
            try {
              this.stop(program);
            } catch (IOException ioe) {
              // ProgramClient#stop calls RestClient, which throws an IOException if the HTTP response code is 400,
              // which can be due to the program already being stopped when calling stop on it.
              // Most likely, there was a race condition that the program stopped between the time we checked its
              // status and calling the stop method.
              LOG.warn("Program {} is already stopped, proceeding even though the following exception is raised.",
                       program, ioe);
            }
            this.waitForStatus(program, ProgramStatus.STOPPED, 60, TimeUnit.SECONDS);
          }
        } catch (ProgramNotFoundException e) {
          // IGNORE
        }
      }
    }
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
  public List<BatchProgramStatus> getStatus(NamespaceId namespace, List<BatchProgram> programs)
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
      throw Throwables.propagate(e.getCause());
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
  public DistributedProgramLiveInfo getLiveInfo(ProgramId program)
    throws IOException, ProgramNotFoundException, UnauthenticatedException, UnauthorizedException {

    String path = String.format("apps/%s/%s/%s/live-info",
                                program.getApplication(), program.getType().getCategoryName(), program.getProgram());
    URL url = config.resolveNamespacedURLV3(program.getNamespaceId(), path);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(program);
    }

    return ObjectResponse.fromJsonBody(response, DistributedProgramLiveInfo.class).getResponseObject();
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
  public int getWorkerInstances(ProgramId worker) throws IOException, NotFoundException,
    UnauthenticatedException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3(worker.getNamespaceId(),
                                            String.format("apps/%s/workers/%s/instances",
                                                          worker.getApplication(), worker.getProgram()));
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
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public void setWorkerInstances(ProgramId worker, int instances) throws IOException, NotFoundException,
    UnauthenticatedException, UnauthorizedException {

    URL url = config.resolveNamespacedURLV3(worker.getNamespaceId(),
                                            String.format("apps/%s/workers/%s/instances",
                                                          worker.getApplication(), worker.getProgram()));
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
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public int getServiceInstances(ServiceId service)
    throws IOException, NotFoundException, UnauthenticatedException, UnauthorizedException {
    URL url = config.resolveNamespacedURLV3(service.getNamespaceId(),
                                            String.format("apps/%s/services/%s/instances",
                                                          service.getApplication(),
                                                          service.getProgram()));
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
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public void setServiceInstances(ServiceId service, int instances)
    throws IOException, NotFoundException, UnauthenticatedException, UnauthorizedException {

    URL url = config.resolveNamespacedURLV3(service.getNamespaceId(),
                                            String.format("apps/%s/services/%s/instances",
                                                          service.getApplication(),
                                                          service.getProgram()));
    HttpRequest request = HttpRequest.put(url).withBody(GSON.toJson(new Instances(instances))).build();
    HttpResponse response = restClient.execute(request, config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(service);
    }
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
  public List<RunRecord> getProgramRuns(ProgramId program, String state,
                                        long startTime, long endTime, int limit)
    throws IOException, NotFoundException, UnauthenticatedException, UnauthorizedException {

    String queryParams = String.format("%s=%s&%s=%d&%s=%d&%s=%d",
                                       Constants.AppFabric.QUERY_PARAM_STATUS, state,
                                       Constants.AppFabric.QUERY_PARAM_START_TIME, startTime,
                                       Constants.AppFabric.QUERY_PARAM_END_TIME, endTime,
                                       Constants.AppFabric.QUERY_PARAM_LIMIT, limit);

    String path = String.format("apps/%s/versions/%s/%s/%s/runs?%s",
                                program.getApplication(), program.getVersion(),
                                program.getType().getCategoryName(),
                                program.getProgram(), queryParams);
    URL url = config.resolveNamespacedURLV3(program.getNamespaceId(), path);

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
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public List<RunRecord> getAllProgramRuns(ProgramId program, long startTime, long endTime, int limit)
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
  public String getProgramLogs(ProgramId program, long start, long stop)
    throws IOException, NotFoundException, UnauthenticatedException, UnauthorizedException {

    String path = String.format("apps/%s/%s/%s/logs?start=%d&stop=%d&escape=false",
                                program.getApplication(), program.getType().getCategoryName(),
                                program.getProgram(), start, stop);
    URL url = config.resolveNamespacedURLV3(program.getNamespaceId(), path);
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
