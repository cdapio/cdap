/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.exception.NotFoundException;
import co.cask.cdap.client.exception.ProgramNotFoundException;
import co.cask.cdap.client.exception.UnAuthorizedAccessTokenException;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.http.HttpMethod;
import co.cask.cdap.common.http.HttpRequest;
import co.cask.cdap.common.http.HttpResponse;
import co.cask.cdap.common.http.ObjectResponse;
import co.cask.cdap.proto.DistributedProgramLiveInfo;
import co.cask.cdap.proto.Instances;
import co.cask.cdap.proto.ProgramStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import com.google.common.base.Charsets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import javax.inject.Inject;

/**
 * Provides ways to interact with CDAP Programs.
 */
public class ProgramClient {

  private static final Gson GSON = new Gson();

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public ProgramClient(ClientConfig config) {
    this.config = config;
    this.restClient = RESTClient.create(config);
  }

  /**
   * Starts a program.
   *
   * @param appId ID of the application that the program belongs to
   * @param programType type of the program
   * @param programName name of the program
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the program with the specified name could not be found
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   */
  public void start(String appId, ProgramType programType, String programName)
    throws IOException, ProgramNotFoundException, UnAuthorizedAccessTokenException {

    URL url = config.resolveURL(String.format("apps/%s/%s/%s/start",
                                              appId, programType.getCategoryName(), programName));
    HttpResponse response = restClient.execute(HttpMethod.POST, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(programType, appId, programName);
    }
  }

  /**
   * Stops a program.
   *
   * @param appId ID of the application that the program belongs to
   * @param programType type of the program
   * @param programName name of the program
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the program with the specified name could not be found
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   */
  public void stop(String appId, ProgramType programType, String programName)
    throws IOException, ProgramNotFoundException, UnAuthorizedAccessTokenException {

    URL url = config.resolveURL(String.format("apps/%s/%s/%s/stop",
                                              appId, programType.getCategoryName(), programName));
    HttpResponse response = restClient.execute(HttpMethod.POST, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(programType, appId, programName);
    }
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
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   */
  public String getStatus(String appId, ProgramType programType, String programName)
    throws IOException, ProgramNotFoundException, UnAuthorizedAccessTokenException {

    URL url = config.resolveURL(String.format("apps/%s/%s/%s/status",
                                              appId, programType.getCategoryName(), programName));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new ProgramNotFoundException(programType, appId, programName);
    }

    return ObjectResponse.fromJsonBody(response, ProgramStatus.class).getResponseObject().getStatus();
  }

  /**
   * Gets the live information of a program. In distributed CDAP,
   * this will contain IDs of the YARN applications that are running the program.
   *
   * @param appId ID of the application that the program belongs to
   * @param programType type of the program
   * @param programName name of the program
   * @return {@ProgramLiveInfo} of the program
   * @throws IOException if a network error occurred
   * @throws ProgramNotFoundException if the program with the specified name could not be found
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   */
  public DistributedProgramLiveInfo getLiveInfo(String appId, ProgramType programType, String programName)
    throws IOException, ProgramNotFoundException, UnAuthorizedAccessTokenException {

    URL url = config.resolveURL(String.format("apps/%s/%s/%s/live-info",
                                              appId, programType.getCategoryName(), programName));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(programType, appId, programName);
    }

    return ObjectResponse.fromJsonBody(response, DistributedProgramLiveInfo.class).getResponseObject();
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
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   */
  public int getFlowletInstances(String appId, String flowId, String flowletId)
    throws IOException, NotFoundException, UnAuthorizedAccessTokenException {

    URL url = config.resolveURL(String.format("apps/%s/flows/%s/flowlets/%s/instances", appId, flowId, flowletId));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException("application or flow or flowlet", appId + "/" + flowId + "/" + flowletId);
    }

    return ObjectResponse.fromJsonBody(response, Instances.class).getResponseObject().getInstances();
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
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   */
  public void setFlowletInstances(String appId, String flowId, String flowletId, int instances)
    throws IOException, NotFoundException, UnAuthorizedAccessTokenException {

    URL url = config.resolveURL(String.format("apps/%s/flows/%s/flowlets/%s/instances", appId, flowId, flowletId));
    HttpRequest request = HttpRequest.put(url).withBody(GSON.toJson(new Instances(instances))).build();

    HttpResponse response = restClient.execute(request, config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException("application or flow or flowlet", appId + "/" + flowId + "/" + flowletId);
    }
  }

  /**
   * Gets the number of instances that a procedure is currently running on.
   *
   * @param appId ID of the application that the procedure belongs to
   * @param procedureId ID of the procedure
   * @return number of instances that the procedure is currently running on
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application or procedure could not be found
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   */
  public int getProcedureInstances(String appId, String procedureId) throws IOException, NotFoundException,
    UnAuthorizedAccessTokenException {

    URL url = config.resolveURL(String.format("apps/%s/procedures/%s/instances", appId, procedureId));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException("application or procedure", appId + "/" + procedureId);
    }

    return ObjectResponse.fromJsonBody(response, Instances.class).getResponseObject().getInstances();
  }

  /**
   * Sets the number of instances that a procedure will run on.
   *
   * @param appId ID of the application that the procedure belongs to
   * @param procedureId ID of the procedure
   * @param instances number of instances for the procedure to run on
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application or procedure could not be found
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   */
  public void setProcedureInstances(String appId, String procedureId, int instances)
    throws IOException, NotFoundException, UnAuthorizedAccessTokenException {

    URL url = config.resolveURL(String.format("apps/%s/procedures/%s/instances", appId, procedureId));
    HttpRequest request = HttpRequest.put(url).withBody(GSON.toJson(new Instances(instances))).build();

    HttpResponse response = restClient.execute(request, config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException("application or procedure", appId + "/" + procedureId);
    }
  }

  /**
   * Gets the number of instances that a service runnable is running on.
   *
   * @param appId ID of the application that the service runnable belongs to
   * @param serviceId ID of the service that the service runnable belongs to
   * @param runnableId ID of the service runnable
   * @return number of instances that the service runnable is running on
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application, service, or runnable could not be found
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   */
  public int getServiceRunnableInstances(String appId, String serviceId, String runnableId)
    throws IOException, NotFoundException, UnAuthorizedAccessTokenException {

    URL url = config.resolveURL(String.format("apps/%s/services/%s/runnables/%s/instances",
                                              appId, serviceId, runnableId));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException("application or service or runnable", appId + "/" + serviceId + "/" + runnableId);
    }

    return ObjectResponse.fromJsonBody(response, Instances.class).getResponseObject().getInstances();
  }

  /**
   * Sets the number of instances that a service runnable is running on.
   *
   * @param appId ID of the application that the service runnable belongs to
   * @param serviceId ID of the service that the service runnable belongs to
   * @param runnableId ID of the service runnable
   * @param instances number of instances for the service runnable to run on
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application, service, or runnable could not be found
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   */
  public void setServiceRunnableInstances(String appId, String serviceId, String runnableId, int instances)
    throws IOException, NotFoundException, UnAuthorizedAccessTokenException {

    URL url = config.resolveURL(String.format("apps/%s/services/%s/runnables/%s/instances",
                                              appId, serviceId, runnableId));
    HttpRequest request = HttpRequest.put(url).withBody(GSON.toJson(new Instances(instances))).build();

    HttpResponse response = restClient.execute(request, config.getAccessToken(), HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException("application or service or runnable", appId + "/" + serviceId + "/" + runnableId);
    }
  }

  /**
   * Gets the run history of a service runnable.
   *
   * @param appId ID of the application that the service runnable belongs to
   * @param serviceId ID of the service that the service runnable belongs to
   * @param runnableId ID of the service runnable
   * @return the run history of the service runnable
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application, service, or runnable could not be found
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   */
  public List<RunRecord> getServiceRunnableHistory(String appId, String serviceId, String runnableId)
    throws IOException, NotFoundException, UnAuthorizedAccessTokenException {

    URL url = config.resolveURL(String.format("apps/%s/services/%s/runnables/%s/history",
                                              appId, serviceId, runnableId));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException("application or service or runnable", appId + "/" + serviceId + "/" + runnableId);
    }

    return ObjectResponse.fromJsonBody(response, new TypeToken<List<RunRecord>>() { }).getResponseObject();
  }

  /**
   * Gets the run history of a program.
   *
   * @param appId ID of the application that the program belongs to
   * @param programType type of the program
   * @param programId ID of the program
   * @return the run history of the program
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application or program could not be found
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   */
  public List<RunRecord> getProgramHistory(String appId, ProgramType programType, String programId)
    throws IOException, NotFoundException, UnAuthorizedAccessTokenException {

    URL url = config.resolveURL(String.format("apps/%s/%s/%s/history",
                                              appId, programType.getCategoryName(), programId));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException("application or " + programType.getCategoryName(), appId + "/" + programId);
    }

    return ObjectResponse.fromJsonBody(response, new TypeToken<List<RunRecord>>() { }).getResponseObject();
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
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   */
  public String getProgramLogs(String appId, ProgramType programType, String programId, long start, long stop)
    throws IOException, NotFoundException, UnAuthorizedAccessTokenException {

    URL url = config.resolveURL(String.format("apps/%s/%s/%s/logs?start=%d&stop=%d",
                                              appId, programType.getCategoryName(), programId, start, stop));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken());
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(programType, appId, programId);
    }

    return new String(response.getResponseBody(), Charsets.UTF_8);
  }

  /**
   * Gets the logs of a service runnable.
   *
   * @param appId ID of the application that the service runnable belongs to
   * @param serviceId ID of the service that the service runnable belongs to
   * @param runnableId ID of the service runnable
   * @param start start time of the time range of desired logs
   * @param stop end time of the time range of desired logs
   * @return the logs of the program
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application, service, or runnable could not be found
   * @throws UnAuthorizedAccessTokenException if the request is not authorized successfully in the gateway server
   */
  public String getServiceRunnableLogs(String appId, String serviceId, String runnableId, long start, long stop)
    throws IOException, NotFoundException, UnAuthorizedAccessTokenException {

    URL url = config.resolveURL(String.format("apps/%s/services/%s/runnables/%s/logs?start=%d&stop=%d",
                                              appId, serviceId, runnableId, start, stop));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException("application or service or runnable", appId + "/" + serviceId + "/" + runnableId);
    }

    return new String(response.getResponseBody(), Charsets.UTF_8);
  }
}
