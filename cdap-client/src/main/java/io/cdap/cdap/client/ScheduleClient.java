/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.util.RESTClient;
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.UnauthenticatedException;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.proto.ProtoConstraintCodec;
import io.cdap.cdap.proto.ProtoTrigger;
import io.cdap.cdap.proto.ProtoTriggerCodec;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.ScheduledRuntime;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.proto.id.WorkflowId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.cdap.common.http.ObjectResponse;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;

/**
 * Provides ways to interact with CDAP Schedules.
 */
@Beta
public class ScheduleClient {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Trigger.class, new ProtoTriggerCodec())
    .registerTypeAdapter(ProtoTrigger.class, new ProtoTriggerCodec())
    .registerTypeAdapter(Constraint.class, new ProtoConstraintCodec())
    .create();

  private static final Type LIST_SCHEDULE_DETAIL_TYPE = new TypeToken<List<ScheduleDetail>>() { }.getType();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public ScheduleClient(ClientConfig config, RESTClient restClient) {
    this.config = config;
    this.restClient = restClient;
  }

  public ScheduleClient(ClientConfig config) {
    this(config, new RESTClient(config));
  }

  /**
   * Add a new schedule for an existing application.
   *
   * @param scheduleId the ID of the schedule to add
   * @param detail the {@link ScheduleDetail} describing the new schedule.
   */
  public void add(ScheduleId scheduleId, ScheduleDetail detail) throws IOException,
    UnauthenticatedException, NotFoundException, UnauthorizedException, AlreadyExistsException {
    doAdd(scheduleId, GSON.toJson(detail));
  }

  public void update(ScheduleId scheduleId, ScheduleDetail detail) throws IOException,
    UnauthenticatedException, NotFoundException, UnauthorizedException, AlreadyExistsException {
    doUpdate(scheduleId, GSON.toJson(detail));
  }

  /**
   * List all schedules for an existing workflow.
   *
   * @param workflow the ID of the workflow
   */
  public List<ScheduleDetail> listSchedules(WorkflowId workflow)
    throws IOException, UnauthenticatedException, NotFoundException, UnauthorizedException {
    return doList(workflow);
  }

  /**
   * Get the next scheduled run time of the program. A program may contain multiple schedules.
   * This method returns the next scheduled runtimes for all the schedules. This method only takes
   + into account schedules based on time. Schedules based on data are ignored.
   *
   * @param workflow Id of the Workflow for which to fetch next run times.
   * @return list of Scheduled runtimes for the Workflow. Empty list if there are no schedules.
   */
  public List<ScheduledRuntime> nextRuntimes(WorkflowId workflow)
    throws IOException, UnauthenticatedException, NotFoundException, UnauthorizedException {

    String path = String.format("apps/%s/workflows/%s/nextruntime", workflow.getApplication(), workflow.getProgram());
    URL url = config.resolveNamespacedURLV3(workflow.getNamespaceId(), path);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException(workflow);
    }

    ObjectResponse<List<ScheduledRuntime>> objectResponse = ObjectResponse.fromJsonBody(
      response, new TypeToken<List<ScheduledRuntime>>() { }.getType(), GSON);
    return objectResponse.getResponseObject();
  }

  public void suspend(ScheduleId scheduleId) throws IOException, UnauthenticatedException, NotFoundException,
    UnauthorizedException {
    String path = String.format("apps/%s/versions/%s/schedules/%s/suspend", scheduleId.getApplication(),
                                scheduleId.getVersion(), scheduleId.getSchedule());
    URL url = config.resolveNamespacedURLV3(scheduleId.getNamespaceId(), path);
    HttpResponse response = restClient.execute(HttpMethod.POST, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException(scheduleId);
    }
  }

  public void resume(ScheduleId scheduleId) throws IOException, UnauthenticatedException, NotFoundException,
    UnauthorizedException {
    String path = String.format("apps/%s/versions/%s/schedules/%s/resume", scheduleId.getApplication(),
                                scheduleId.getVersion(), scheduleId.getSchedule());
    URL url = config.resolveNamespacedURLV3(scheduleId.getNamespaceId(), path);
    HttpResponse response = restClient.execute(HttpMethod.POST, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException(scheduleId);
    }
  }

  /**
   * Delete an existing schedule.
   *
   * @param scheduleId the ID of the schedule to be deleted
   */
  public void delete(ScheduleId scheduleId) throws IOException, UnauthenticatedException, NotFoundException,
    UnauthorizedException {
    String path = String.format("apps/%s/versions/%s/schedules/%s", scheduleId.getApplication(),
                                scheduleId.getVersion(), scheduleId.getSchedule());
    URL url = config.resolveNamespacedURLV3(scheduleId.getNamespaceId(), path);
    HttpResponse response = restClient.execute(HttpMethod.DELETE, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException(scheduleId);
    }
  }

  public String getStatus(ScheduleId scheduleId) throws IOException, UnauthenticatedException, NotFoundException,
    UnauthorizedException {
    String path = String.format("apps/%s/versions/%s/schedules/%s/status", scheduleId.getApplication(),
                                scheduleId.getVersion(), scheduleId.getSchedule());
    URL url = config.resolveNamespacedURLV3(scheduleId.getParent().getParent(), path);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException(scheduleId);
    }

    Map<String, String> responseObject
      = ObjectResponse.<Map<String, String>>fromJsonBody(response, MAP_STRING_STRING_TYPE, GSON).getResponseObject();
    return responseObject.get("status");
  }

  /**
   * Update schedules which were suspended between startTimeMillis and endTimeMillis.
   *
   * @param namespaceId the namespace in which to restart schedules
   * @param startTimeMillis lower bound in millis of the update time for schedules (inclusive)
   * @param endTimeMillis upper bound in millis of the update time for schedules (exclusive)
   */
  public void reEnableSuspendedSchedules(NamespaceId namespaceId, long startTimeMillis, long endTimeMillis)
    throws IOException, UnauthorizedException, UnauthenticatedException, NotFoundException {
    String path =
      String.format("schedules/re-enable?start-time-millis=%d&end-time-millis=%d", startTimeMillis, endTimeMillis);
    URL url = config.resolveNamespacedURLV3(namespaceId.getNamespaceId(), path);
    HttpResponse response = restClient.execute(HttpMethod.PUT, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException(namespaceId);
    }
  }

  /*------------ private helpers ---------------------*/

  private void doAdd(ScheduleId scheduleId, String json)  throws IOException,
    UnauthenticatedException, NotFoundException, UnauthorizedException, AlreadyExistsException {

    String path = String.format("apps/%s/versions/%s/schedules/%s",
                                scheduleId.getApplication(), scheduleId.getVersion(), scheduleId.getSchedule());
    URL url = config.resolveNamespacedURLV3(scheduleId.getNamespaceId(), path);
    HttpRequest request = HttpRequest.put(url).withBody(json).build();
    HttpResponse response = restClient.execute(request, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND,
                                               HttpURLConnection.HTTP_CONFLICT);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException(scheduleId);
    } else if (HttpURLConnection.HTTP_CONFLICT == response.getResponseCode()) {
      throw new AlreadyExistsException(scheduleId);
    }
  }

  private void doUpdate(ScheduleId scheduleId, String json) throws IOException,
    UnauthenticatedException, NotFoundException, UnauthorizedException, AlreadyExistsException {

    String path = String.format("apps/%s/versions/%s/schedules/%s/update",
                                scheduleId.getApplication(), scheduleId.getVersion(), scheduleId.getSchedule());
    URL url = config.resolveNamespacedURLV3(scheduleId.getNamespaceId(), path);
    HttpRequest request = HttpRequest.post(url).withBody(json).build();
    HttpResponse response = restClient.execute(request, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException(scheduleId);
    }
  }

  private List<ScheduleDetail> doList(WorkflowId workflow)
    throws IOException, UnauthenticatedException, NotFoundException, UnauthorizedException {

    String path = String.format("apps/%s/workflows/%s/schedules", workflow.getApplication(), workflow.getProgram());
    URL url = config.resolveNamespacedURLV3(workflow.getNamespaceId(), path);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException(workflow);
    }

    ObjectResponse<List<ScheduleDetail>> objectResponse =
      ObjectResponse.fromJsonBody(response, LIST_SCHEDULE_DETAIL_TYPE, GSON);
    return objectResponse.getResponseObject();
  }

}
