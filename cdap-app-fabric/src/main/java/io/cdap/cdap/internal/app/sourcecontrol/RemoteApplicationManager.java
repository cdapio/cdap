/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.sourcecontrol;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.metadata.RemoteApplicationDetailFetcher;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.app.AppVersion;
import io.cdap.cdap.proto.app.MarkLatestAppsRequest;
import io.cdap.cdap.proto.app.UpdateMultiSourceControlMetaReqeust;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.sourcecontrol.ApplicationManager;
import io.cdap.cdap.sourcecontrol.SourceControlException;
import io.cdap.cdap.sourcecontrol.operationrunner.PullAppResponse;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.List;

/**
 * Remote implementation of {@link ApplicationManager} which calls app-fabric apis.
 */
public class RemoteApplicationManager implements ApplicationManager {

  private final RemoteClient remoteClient;
  private final RemoteApplicationDetailFetcher appDetailsFetcher;

  private static final Gson GSON = new Gson();

  @Inject
  RemoteApplicationManager(RemoteClientFactory remoteClientFactory,
      RemoteApplicationDetailFetcher appDetailsFetcher) {
    this.remoteClient = remoteClientFactory.createRemoteClient(
        Constants.Service.APP_FABRIC_HTTP,
        new DefaultHttpRequestConfig(false),
        Constants.Gateway.INTERNAL_API_VERSION_3);
    this.appDetailsFetcher = appDetailsFetcher;
  }

  @Override
  public ApplicationId deployApp(ApplicationReference appRef, PullAppResponse<?> pullDetails)
      throws SourceControlException, NotFoundException, IOException {
    String url = String.format("namespaces/%s/apps/%s?skipMarkingLatest=true",
        appRef.getNamespace(), appRef.getApplication());
    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.PUT, url)
        .withBody(GSON.toJson(pullDetails.getAppRequest()));
    HttpResponse httpResponse;
    httpResponse = execute(requestBuilder.build());
    ApplicationRecord response = GSON.fromJson(httpResponse.getResponseBodyAsString(),
        ApplicationRecord.class);
    return appRef.app(response.getAppVersion());
  }

  @Override
  public void markAppVersionsLatest(NamespaceId namespace, List<AppVersion> apps)
      throws SourceControlException, NotFoundException, IOException {
    MarkLatestAppsRequest markLatestRequest = new MarkLatestAppsRequest(apps);
    String url = String.format("namespaces/%s/apps/markLatest", namespace.getEntityName());
    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.POST, url)
        .withBody(GSON.toJson(markLatestRequest));
    execute(requestBuilder.build());
  }

  @Override
  public void updateSourceControlMeta(NamespaceId namespace,
      UpdateMultiSourceControlMetaReqeust metas)
      throws SourceControlException, NotFoundException, IOException {
    String url = String.format("namespaces/%s/apps/updateSourceControlMeta",
        namespace.getEntityName());
    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.POST, url)
        .withBody(GSON.toJson(metas));
    execute(requestBuilder.build());
  }

  @Override
  public ApplicationDetail get(ApplicationReference appRef)
      throws IOException, NotFoundException, UnauthorizedException {
    return appDetailsFetcher.get(appRef);
  }

  private HttpResponse execute(HttpRequest request)
      throws SourceControlException, NotFoundException, IOException {
    HttpResponse httpResponse = remoteClient.execute(request);
    if (httpResponse.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(httpResponse.getResponseBodyAsString());
    }
    if (httpResponse.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new SourceControlException(httpResponse.getResponseBodyAsString());
    }
    return httpResponse;
  }
}
