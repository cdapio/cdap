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

package io.cdap.cdap.logging.gateway.handlers;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.net.HttpURLConnection;

/**
 * Fetch {@link RunRecordDetail} via internal REST API calls
 */
public class RemoteProgramRunRecordFetcher implements ProgramRunRecordFetcher {
  private static final Gson GSON = new Gson();

  private final RemoteClient remoteClient;

  @Inject
  public RemoteProgramRunRecordFetcher(DiscoveryServiceClient discoveryClient) {
    this.remoteClient = new RemoteClient(discoveryClient,
                                         Constants.Service.APP_FABRIC_HTTP,
                                         new DefaultHttpRequestConfig(false),
                                         Constants.Gateway.INTERNAL_API_VERSION_3);
  }

  /**
   * Return {@link RunRecordDetail} for the given {@link ProgramRunId}
   * @param runId for which to fetch {@link RunRecordDetail}
   * @return {@link RunRecordDetail}
   * @throws IOException if failed to fetch {@link RunRecordDetail}
   * @throws NotFoundException if the program or runid is not found
   */
  public RunRecordDetail getRunRecordMeta(ProgramRunId runId) throws IOException, NotFoundException {
    String url = String.format("namespaces/%s/apps/%s/versions/%s/%s/%s/runs/%s",
                               runId.getNamespace(),
                               runId.getApplication(),
                               runId.getVersion(),
                               runId.getType().getCategoryName(),
                               runId.getProgram(),
                               runId.getRun());
    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.GET, url);
    HttpResponse httpResponse;
    httpResponse = execute(requestBuilder.build());
    return RunRecordDetail.builder(GSON.fromJson(httpResponse.getResponseBodyAsString(), RunRecordDetail.class))
      .setProgramRunId(runId)
      .build();
  }

  private HttpResponse execute(HttpRequest request) throws IOException, NotFoundException {
    HttpResponse httpResponse = remoteClient.execute(request);
    if (httpResponse.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(httpResponse.getResponseBodyAsString());
    }
    if (httpResponse.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException(String.format("Request failed %s", httpResponse.getResponseBodyAsString()));
    }
    return httpResponse;
  }
}
