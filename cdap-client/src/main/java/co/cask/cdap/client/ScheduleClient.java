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

import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.common.exception.UnauthorizedException;
import co.cask.cdap.proto.ProgramStatus;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;

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
public class ScheduleClient {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(ScheduleSpecification.class, new ScheduleSpecificationDeserializer())
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

  public List<ScheduleSpecification> list(String appId, String workflowId)
    throws IOException, UnauthorizedException, NotFoundException {

    String path = String.format("apps/%s/workflows/%s/schedules", appId, workflowId);
    URL url = config.resolveNamespacedURLV3(path);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException("workflow", workflowId);
    }

    ObjectResponse<List<ScheduleSpecification>> objectResponse = ObjectResponse.fromJsonBody(
      response, new TypeToken<List<ScheduleSpecification>>() { }.getType(), GSON);
    return objectResponse.getResponseObject();
  }

  public void suspend(String appId, String scheduleId) throws IOException, UnauthorizedException, NotFoundException {
    String path = String.format("apps/%s/schedules/%s/suspend", appId, scheduleId);
    URL url = config.resolveNamespacedURLV3(path);
    HttpResponse response = restClient.execute(HttpMethod.POST, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException("schedule", scheduleId);
    }
  }

  public void resume(String appId, String scheduleId) throws IOException, UnauthorizedException, NotFoundException {
    String path = String.format("apps/%s/schedules/%s/resume", appId, scheduleId);
    URL url = config.resolveNamespacedURLV3(path);
    HttpResponse response = restClient.execute(HttpMethod.POST, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException("schedule", scheduleId);
    }
  }

  public String getStatus(String appId, String scheduleId)
    throws IOException, UnauthorizedException, NotFoundException {

    String path = String.format("apps/%s/schedules/%s/status", appId, scheduleId);
    URL url = config.resolveNamespacedURLV3(path);
    HttpResponse response = restClient.execute(HttpMethod.GET, url, config.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException("schedule", scheduleId);
    }
    return ObjectResponse.fromJsonBody(response, ProgramStatus.class).getResponseObject().getStatus();
  }

  /**
   * Deserializes {@link ScheduleSpecification}.
   */
  private static class ScheduleSpecificationDeserializer implements JsonDeserializer<ScheduleSpecification> {

    @Override
    public ScheduleSpecification deserialize(JsonElement json, Type typeOfT,
                                             JsonDeserializationContext context) throws JsonParseException {
      JsonObject object = json.getAsJsonObject();
      JsonObject scheduleJson = object.getAsJsonObject("schedule");
      String scheduleType = object.get("scheduleType").getAsString();

      Schedule schedule;
      try {
        Class<? extends Schedule> scheduleClass = (Class<? extends Schedule>) Class.forName(scheduleType);
        schedule = context.deserialize(scheduleJson, scheduleClass);
      } catch (Exception e) {
        throw new JsonParseException("Unknown scheduleType: " + scheduleType, e);
      }

      ScheduleProgramInfo program = context.deserialize(object.get("program"), ScheduleProgramInfo.class);
      Map<String, String> properties = context.deserialize(object.get("properties"),
                                                           new TypeToken<Map<String, String>>() { }.getType());

      return new ScheduleSpecification(schedule, program, properties);
    }
  }
}
