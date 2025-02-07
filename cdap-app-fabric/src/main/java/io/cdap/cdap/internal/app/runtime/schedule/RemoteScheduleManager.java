/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.schedule;

import com.google.inject.Inject;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.Gateway;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.gateway.handlers.util.ProgramHandlerUtil;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import java.io.IOException;
import java.net.HttpURLConnection;

/**
 * {@code ScheduleManager} to manage program schedules. This class uses remote client to communicate with
 * {@code ProgramScheduleHttpHandler}.
 */
public class RemoteScheduleManager extends ScheduleManager {

  private RemoteClient remoteClient;

  /**
   * Parameterized constructor for RemoteScheduleManager.
   *
   * @param transactionRunner TransactionRunner
   * @param messagingService MessagingService
   * @param cConf CConfiguration
   * @param clientFactory RemoteClientFactory
   */
  @Inject
  public RemoteScheduleManager(TransactionRunner transactionRunner, MessagingService messagingService,
      CConfiguration cConf, RemoteClientFactory clientFactory) {
    super(transactionRunner, messagingService, cConf);
    this.remoteClient = clientFactory.createRemoteClient(Constants.Service.APP_FABRIC_PROCESSOR,
        new DefaultHttpRequestConfig(false),
        Gateway.API_VERSION_3);
  }

  @Override
  public void addSchedules(Iterable<? extends ProgramSchedule> schedules)
      throws BadRequestException, NotFoundException, IOException, ConflictException {
    for (ProgramSchedule schedule : schedules) {
      ScheduleId scheduleId = schedule.getScheduleId();
      String url = String.format("namespaces/%s/apps/%s/schedules/%s",
          scheduleId.getNamespace(),
          scheduleId.getApplication(),
          scheduleId.getSchedule());
      HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.PUT, url);
      ScheduleDetail scheduleDetail = schedule.toScheduleDetail();
      requestBuilder.withBody(ProgramHandlerUtil.toJson(scheduleDetail, ScheduleDetail.class));
      execute(requestBuilder.build());
    }
  }

  @Override
  public void deleteSchedule(ScheduleId scheduleId)
      throws NotFoundException, BadRequestException, IOException, ConflictException {
    String url = String.format("namespaces/%s/apps/%s/schedules/%s",
        scheduleId.getNamespace(),
        scheduleId.getApplication(),
        scheduleId.getSchedule());
    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.DELETE, url);
    execute(requestBuilder.build());
  }

  private HttpResponse execute(HttpRequest request)
      throws IOException, NotFoundException, UnauthorizedException, BadRequestException, ConflictException {
      HttpResponse httpResponse = remoteClient.execute(request);
    switch (httpResponse.getResponseCode()) {
      case HttpURLConnection.HTTP_OK:
        return httpResponse;
      case HttpURLConnection.HTTP_BAD_REQUEST:
        throw new BadRequestException(httpResponse.getResponseBodyAsString());
      case HttpURLConnection.HTTP_NOT_FOUND:
        throw new NotFoundException(httpResponse.getResponseBodyAsString());
      case HttpURLConnection.HTTP_CONFLICT:
        throw new ConflictException(httpResponse.getResponseBodyAsString());
      default:
        throw new IOException(
            String.format("Request failed %s", httpResponse.getResponseBodyAsString()));
    }
  }
}
