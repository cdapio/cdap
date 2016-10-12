/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ScheduledRuntime;
import co.cask.cdap.proto.codec.ScheduleSpecificationCodec;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

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
    .registerTypeAdapter(ScheduleSpecification.class, new ScheduleSpecificationCodec())
    .create();

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

  public List<ScheduleSpecification> list(Id.Workflow workflow)
    throws IOException, UnauthenticatedException, NotFoundException, UnauthorizedException {

    String path = String.format("apps/%s/workflows/%s/schedules", workflow.getApplicationId(), workflow.getId());
    URL url = config.resolveNamespacedURLV3(workflow.getNamespace(), path);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException(workflow.toEntityId());
    }

    ObjectResponse<List<ScheduleSpecification>> objectResponse = ObjectResponse.fromJsonBody(
      response, new TypeToken<List<ScheduleSpecification>>() { }.getType(), GSON);
    return objectResponse.getResponseObject();
  }

  /**
   * Get the next scheduled run time of the program. A program may contain multiple schedules.
   * This method returns the next scheduled runtimes for all the schedules. This method only takes
   + into account {@link Schedule}s based on time. For schedules based on data, an empty list will
   + be returned.
   *
   * @param workflow Id of the Workflow for which to fetch next run times.
   * @return list of Scheduled runtimes for the Workflow. Empty list if there are no schedules.
   */
  public List<ScheduledRuntime> nextRuntimes(Id.Workflow workflow)
    throws IOException, UnauthenticatedException, NotFoundException, UnauthorizedException {

    String path = String.format("apps/%s/workflows/%s/nextruntime", workflow.getApplicationId(), workflow.getId());
    URL url = config.resolveNamespacedURLV3(workflow.getNamespace(), path);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException(workflow.toEntityId());
    }

    ObjectResponse<List<ScheduledRuntime>> objectResponse = ObjectResponse.fromJsonBody(
      response, new TypeToken<List<ScheduledRuntime>>() { }.getType(), GSON);
    return objectResponse.getResponseObject();
  }

  public void suspend(Id.Schedule schedule)
    throws IOException, UnauthenticatedException, NotFoundException, UnauthorizedException {
    String path = String.format("apps/%s/schedules/%s/suspend", schedule.getApplication().getId(), schedule.getId());
    URL url = config.resolveNamespacedURLV3(schedule.getNamespace(), path);
    HttpResponse response = restClient.execute(HttpMethod.POST, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException(schedule.toEntityId());
    }
  }

  public void suspend(ScheduleId scheduleId) throws IOException, UnauthenticatedException, NotFoundException,
    UnauthorizedException {
    String path = String.format("apps/%s/versions/%s/schedules/%s/suspend", scheduleId.getApplication(),
                                scheduleId.getVersion(), scheduleId.getSchedule());
    URL url = config.resolveNamespacedURLV3(scheduleId.toId().getNamespace(), path);
    HttpResponse response = restClient.execute(HttpMethod.POST, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException(scheduleId);
    }
  }

  public void resume(Id.Schedule schedule)
    throws IOException, UnauthenticatedException, NotFoundException, UnauthorizedException {
    String path = String.format("apps/%s/schedules/%s/resume", schedule.getApplication().getId(), schedule.getId());
    URL url = config.resolveNamespacedURLV3(schedule.getNamespace(), path);
    HttpResponse response = restClient.execute(HttpMethod.POST, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException(schedule.toEntityId());
    }
  }

  public void resume(ScheduleId scheduleId) throws IOException, UnauthenticatedException, NotFoundException,
    UnauthorizedException {
    String path = String.format("apps/%s/versions/%s/schedules/%s/resume", scheduleId.getApplication(),
                                scheduleId.getVersion(), scheduleId.getSchedule());
    URL url = config.resolveNamespacedURLV3(scheduleId.toId().getNamespace(), path);
    HttpResponse response = restClient.execute(HttpMethod.POST, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException(scheduleId);
    }
  }

  public String getStatus(Id.Schedule schedule)
    throws IOException, UnauthenticatedException, NotFoundException, UnauthorizedException {
    String path = String.format("apps/%s/schedules/%s/status", schedule.getApplication().getId(), schedule.getId());
    URL url = config.resolveNamespacedURLV3(schedule.getNamespace(), path);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException(schedule.toEntityId());
    }

    Map<String, String> responseObject
      = ObjectResponse.<Map<String, String>>fromJsonBody(response, MAP_STRING_STRING_TYPE, GSON).getResponseObject();
    return responseObject.get("status");
  }

  public String getStatus(ScheduleId scheduleId) throws IOException, UnauthenticatedException, NotFoundException,
    UnauthorizedException {
    String path = String.format("apps/%s/versions/%s/schedules/%s/status", scheduleId.getApplication(),
                                scheduleId.getVersion(), scheduleId.getSchedule());
    URL url = config.resolveNamespacedURLV3(scheduleId.toId().getNamespace(), path);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException(scheduleId);
    }

    Map<String, String> responseObject
      = ObjectResponse.<Map<String, String>>fromJsonBody(response, MAP_STRING_STRING_TYPE, GSON).getResponseObject();
    return responseObject.get("status");
  }
}
