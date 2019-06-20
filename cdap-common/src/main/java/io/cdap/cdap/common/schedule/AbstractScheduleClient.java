/*
 *
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.common.schedule;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.UnauthenticatedException;
import io.cdap.cdap.common.client.AbstractClient;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.proto.ProtoConstraintCodec;
import io.cdap.cdap.proto.ProtoTrigger;
import io.cdap.cdap.proto.ProtoTriggerCodec;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.ScheduledRuntime;
import io.cdap.cdap.proto.id.WorkflowId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.cdap.common.http.ObjectResponse;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Common implementation of methods to interact with app fabric service over HTTP.
 */
public abstract class AbstractScheduleClient extends AbstractClient {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Trigger.class, new ProtoTriggerCodec())
    .registerTypeAdapter(ProtoTrigger.class, new ProtoTriggerCodec())
    .registerTypeAdapter(Constraint.class, new ProtoConstraintCodec())
    .create();

  private static final Type LIST_SCHEDULE_DETAIL_TYPE = new TypeToken<List<ScheduleDetail>>() {
  }.getType();

  public AbstractScheduleClient(DiscoveryServiceClient discoveryClient) {
    super(discoveryClient);
  }

  /**
   * List all schedules for an existing workflow.
   *
   * @param workflow the ID of the workflow
   */
  public List<ScheduleDetail> listSchedules(WorkflowId workflow)
    throws IOException, UnauthenticatedException, NotFoundException, UnauthorizedException, BadRequestException {
    return doList(workflow);
  }

  private List<ScheduleDetail> doList(WorkflowId workflow)
    throws IOException, UnauthenticatedException, NotFoundException, UnauthorizedException, BadRequestException {
    String path = String.format("namespaces/%s/apps/%s/workflows/%s/schedules",
                                workflow.getNamespace(), workflow.getApplication(), workflow.getProgram());
    HttpResponse response = makeRequest(path, HttpMethod.GET, null);

    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException(workflow);
    }

    ObjectResponse<List<ScheduleDetail>> objectResponse =
      ObjectResponse.fromJsonBody(response, LIST_SCHEDULE_DETAIL_TYPE, GSON);

    return objectResponse.getResponseObject();
  }

  /**
   * Get the next scheduled run time of the program. A program may contain multiple schedules. This method returns the
   * next scheduled runtimes for all the schedules. This method only takes + into account schedules based on time.
   * Schedules based on data are ignored.
   *
   * @param workflow Id of the Workflow for which to fetch next run times.
   * @return list of Scheduled runtimes for the Workflow. Empty list if there are no schedules.
   */
  public List<ScheduledRuntime> nextRuntimes(WorkflowId workflow)
    throws IOException, NotFoundException, UnauthorizedException, BadRequestException {

    String path = String.format("namespaces/%s/apps/%s/workflows/%s/nextruntime",
                                workflow.getNamespace(), workflow.getApplication(), workflow.getProgram());
    HttpResponse response = makeRequest(path, HttpMethod.GET, null);

    if (HttpURLConnection.HTTP_NOT_FOUND == response.getResponseCode()) {
      throw new NotFoundException(workflow);
    }

    ObjectResponse<List<ScheduledRuntime>> objectResponse = ObjectResponse.fromJsonBody(
      response, new TypeToken<List<ScheduledRuntime>>() {
      }.getType(), GSON);

    return objectResponse.getResponseObject();
  }

  private HttpResponse makeRequest(String path, HttpMethod httpMethod, @Nullable String body)
    throws IOException, BadRequestException, UnauthorizedException {
    URL url = resolve(path);
    HttpRequest.Builder builder = HttpRequest.builder(httpMethod, url);
    if (body != null) {
      builder.withBody(body);
    }
    HttpResponse response = execute(builder.build(),
                                    HttpURLConnection.HTTP_BAD_REQUEST, HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException(response.getResponseBodyAsString());
    }
    return response;
  }

}
