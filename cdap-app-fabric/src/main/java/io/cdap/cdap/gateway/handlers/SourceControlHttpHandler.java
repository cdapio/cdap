/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.internal.app.RemoteTaskExecutor;
import io.cdap.cdap.internal.app.worker.GitRepoInitializationTask;
import io.cdap.cdap.internal.app.worker.SourceControlInfo;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * {@link io.cdap.http.HttpHandler} for managing source control operations for pipelines.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class SourceControlHttpHandler extends AbstractHttpHandler {
  private static final Gson GSON = new GsonBuilder().create();
  // Gson for decoding request
  private static final Gson DECODE_GSON = new GsonBuilder()
    .registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory())
    .create();
  private static final Logger LOG = LoggerFactory.getLogger(AppLifecycleHttpHandler.class);

  private final RemoteTaskExecutor remoteTaskExecutor;
  private final CConfiguration cConf;

  @Inject
  public SourceControlHttpHandler(CConfiguration cConf, MetricsCollectionService metricsCollectionService,
                                  RemoteClientFactory remoteClientFactory) {
    this.cConf = cConf;
    int connectTimeout = cConf.getInt(Constants.SystemWorker.HTTP_CLIENT_CONNECTION_TIMEOUT_MS);
    int readTimeout = cConf.getInt(Constants.SystemWorker.HTTP_CLIENT_READ_TIMEOUT_MS);
    HttpRequestConfig httpRequestConfig = new HttpRequestConfig(connectTimeout, readTimeout, false);
    this.remoteTaskExecutor = new RemoteTaskExecutor(cConf, metricsCollectionService, remoteClientFactory,
                                                     RemoteTaskExecutor.Type.SYSTEM_WORKER, httpRequestConfig);
  }

  public static final class RepositoryInfo {
    public String getRepositoryLink() {
      return repositoryLink;
    }

    @Nullable
    public String getRepositoryRootPath() {
      return repositoryRootPath;
    }

    public String getAccessToken() {
      return accessToken;
    }

    private final String repositoryLink;
    @Nullable
    private final String repositoryRootPath;
    private final String accessToken;


    public RepositoryInfo(String repositoryLink, @Nullable String repositoryRootPath, String accessToken) {
      this.repositoryLink = repositoryLink;
      this.repositoryRootPath = repositoryRootPath;
      this.accessToken = accessToken;
    }
  }

  @POST
  @Path("/namespaces/{namespace-id}/repository")
  public void initializeRepository(FullHttpRequest request, HttpResponder responder,
                                   @PathParam("namespace-id") String namespace) throws Exception {
    RepositoryInfo repositoryInfo;
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(request.content()), StandardCharsets.UTF_8)) {
      repositoryInfo = DECODE_GSON.fromJson(reader, RepositoryInfo.class);
    } catch (IOException e) {
      LOG.error("Error reading request to update repository for namespace {}.", namespace, e);
      throw new IOException("Error reading request body.");
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Request body is invalid json: " + e.getMessage());
    }

    LOG.debug("Running task to initialize repository for namespace {}", namespace);
    RunnableTaskRequest taskRequest = RunnableTaskRequest.getBuilder(GitRepoInitializationTask.class.getName())
      .withParam(GSON.toJson(new SourceControlInfo.Builder()
                               .setRepositoryLink(repositoryInfo.getRepositoryLink())
                               .setRepositoryProvider(SourceControlInfo.RepositoryProvider.GITHUB)
                               .setNamespaceId(namespace)
                               .setRepositoryRootPath(repositoryInfo.getRepositoryRootPath())
                               .setAccessToken(repositoryInfo.getAccessToken())
                               .build())).build();
    remoteTaskExecutor.runTask(taskRequest);
  }

}
