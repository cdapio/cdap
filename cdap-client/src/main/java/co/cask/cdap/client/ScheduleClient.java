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

package co.cask.cdap.client;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramStatus;
import co.cask.cdap.proto.codec.ScheduleSpecificationCodec;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import javax.inject.Inject;

/**
 * Provides ways to interact with CDAP Schedules.
 */
@Beta
public class ScheduleClient {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(ScheduleSpecification.class, new ScheduleSpecificationCodec())
    .create();

  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public ScheduleClient(ClientConfig config, RESTClient restClient) {
    this.config = config;
    this.restClient = restClient;
  }

  public ScheduleClient(ClientConfig config) {
    this.config = config;
    this.restClient = new RESTClient(config);
  }

  /**
   * @deprecated As of 3.1, use {@link #list(Id.Workflow)} instead.
   */
  @Deprecated
  public List<ScheduleSpecification> list(String appId, String workflowId)
    throws IOException, UnauthorizedException, NotFoundException {

    return list(Id.Workflow.from(config.getNamespace(), appId, workflowId));
  }

  /**
   * @deprecated As of 3.1, use {@link #suspend(Id.Schedule)} instead.
   */
  @Deprecated
  public void suspend(String appId, String scheduleId) throws IOException, UnauthorizedException, NotFoundException {
    suspend(Id.Schedule.from(config.getNamespace(), appId, scheduleId));
  }

  /**
   * @deprecated As of 3.1, use {@link #resume(Id.Schedule)} instead.
   */
  @Deprecated
  public void resume(String appId, String scheduleId) throws IOException, UnauthorizedException, NotFoundException {
    resume(Id.Schedule.from(config.getNamespace(), appId, scheduleId));
  }

  /**
   * @deprecated As of 3.1, use {@link #getStatus(Id.Schedule)} instead.
   */
  @Deprecated
  public String getStatus(String appId, String scheduleId)
    throws IOException, UnauthorizedException, NotFoundException {

    return getStatus(Id.Schedule.from(config.getNamespace(), appId, scheduleId));
  }

  public List<ScheduleSpecification> list(Id.Workflow workflow)
    throws IOException, UnauthorizedException, NotFoundException {

    String path = String.format("apps/%s/workflows/%s/schedules", workflow.getApplicationId(), workflow.getId());
    URL url = config.resolveNamespacedURLV3(workflow.getNamespace(), path);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException(workflow);
    }

    ObjectResponse<List<ScheduleSpecification>> objectResponse = ObjectResponse.fromJsonBody(
      response, new TypeToken<List<ScheduleSpecification>>() { }.getType(), GSON);
    return objectResponse.getResponseObject();
  }

  public void suspend(Id.Schedule schedule) throws IOException, UnauthorizedException, NotFoundException {
    String path = String.format("apps/%s/schedules/%s/suspend", schedule.getApplication().getId(), schedule.getId());
    URL url = config.resolveNamespacedURLV3(schedule.getNamespace(), path);
    HttpResponse response = restClient.execute(HttpMethod.POST, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException(schedule);
    }
  }

  public void resume(Id.Schedule schedule) throws IOException, UnauthorizedException, NotFoundException {
    String path = String.format("apps/%s/schedules/%s/resume", schedule.getApplication().getId(), schedule.getId());
    URL url = config.resolveNamespacedURLV3(schedule.getNamespace(), path);
    HttpResponse response = restClient.execute(HttpMethod.POST, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException(schedule);
    }
  }

  public String getStatus(Id.Schedule schedule) throws IOException, UnauthorizedException, NotFoundException {
    String path = String.format("apps/%s/schedules/%s/status", schedule.getApplication().getId(), schedule.getId());
    URL url = config.resolveNamespacedURLV3(schedule.getNamespace(), path);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException(schedule);
    }
    return ObjectResponse.fromJsonBody(response, ProgramStatus.class).getResponseObject().getStatus();
  }
}
