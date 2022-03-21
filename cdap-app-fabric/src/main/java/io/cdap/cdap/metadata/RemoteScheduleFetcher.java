/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.metadata;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.ProgramNotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.internal.app.runtime.schedule.ScheduleNotFoundException;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.SatisfiableTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TriggerCodec;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.cdap.common.http.ObjectResponse;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.util.List;

/**
 * Fetch schedules via REST API calls
 */
public class RemoteScheduleFetcher implements ScheduleFetcher {
  protected static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Trigger.class, new TriggerCodec())
    .registerTypeAdapter(SatisfiableTrigger.class, new TriggerCodec())
    .create();

  private static final Type SCHEDULE_DETAIL_LIST_TYPE = new TypeToken<List<ScheduleDetail>>() { }.getType();

  private final RemoteClient remoteClient;

  @Inject
  public RemoteScheduleFetcher(RemoteClientFactory remoteClientFactory) {
    this.remoteClient = remoteClientFactory.createRemoteClient(
      Constants.Service.APP_FABRIC_HTTP,
      new DefaultHttpRequestConfig(false), Constants.Gateway.API_VERSION_3);
  }

  /**
   * Get the schedule identified by the given schedule id
   */
  @Override
  public ScheduleDetail get(ScheduleId scheduleId)
    throws IOException, ScheduleNotFoundException, UnauthorizedException {
    String url = String.format(
      "namespaces/%s/apps/%s/versions/%s/schedules/%s",
      scheduleId.getNamespace(), scheduleId.getApplication(), scheduleId.getVersion(), scheduleId.getSchedule());
    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.GET, url);
    HttpResponse httpResponse;
    try {
      httpResponse = execute(requestBuilder.build());
    } catch (NotFoundException e) {
      throw new ScheduleNotFoundException(scheduleId);
    }
    return GSON.fromJson(httpResponse.getResponseBodyAsString(), ScheduleDetail.class);
  }

  /**
   * Get the list of schedules for the given program id
   */
  @Override
  public List<ScheduleDetail> list(ProgramId programId)
    throws IOException, ProgramNotFoundException, UnauthorizedException {
    String url = String.format("namespaces/%s/apps/%s/versions/%s/schedules",
                               programId.getNamespace(), programId.getApplication(), programId.getVersion());
    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.GET, url);
    HttpResponse httpResponse = null;
    try {
      httpResponse = execute(requestBuilder.build());
    } catch (NotFoundException e) {
      throw new ProgramNotFoundException(programId);
    }
    ObjectResponse<List<ScheduleDetail>> objectResponse =
      ObjectResponse.fromJsonBody(httpResponse, SCHEDULE_DETAIL_LIST_TYPE, GSON);
    return objectResponse.getResponseObject();
  }

  // TODO: refactor out into a util function that can be shared by RemoteApplicationDetailFetcher
  //       RemotePreferencesFetcherInternal and RemoteScheduleFetcher
  private HttpResponse execute(HttpRequest request) throws IOException, NotFoundException, UnauthorizedException {
    HttpResponse httpResponse = remoteClient.execute(request);
    if (httpResponse.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(httpResponse.getResponseBodyAsString());
    }
    if (httpResponse.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException(httpResponse.getResponseBodyAsString());
    }
    return httpResponse;
  }

}
