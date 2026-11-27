/*
 * Copyright © 2021-2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.TaskWorker;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.internal.remote.TaskWorkerHttpHandlerInternal;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.cdap.common.service.Services;
import io.cdap.cdap.gateway.handlers.PingHandler;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.security.auth.AccessToken;
import io.cdap.cdap.security.auth.InvalidTokenException;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.UserIdentity;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.ChannelPipelineModifier;
import io.cdap.http.HttpHandler;
import io.cdap.http.NettyHttpService;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpRequest;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Launches an HTTP server for receiving and handling {@link RunnableTask}.
 */
public class TaskWorkerService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(TaskWorkerService.class);

  private final DiscoveryService discoveryService;
  private final NettyHttpService httpService;
  private final boolean callerAuthEnabled;
  @Nullable
  private final TokenManager tokenManager;
  @Nullable
  private final Codec<AccessToken> accessTokenCodec;
  private Cancellable cancelDiscovery;
  private InetSocketAddress bindAddress;

  TaskWorkerService(CConfiguration cConf,
      SConfiguration sConf,
      DiscoveryService discoveryService,
      DiscoveryServiceClient discoveryServiceClient,
      MetricsCollectionService metricsCollectionService,
      CommonNettyHttpServiceFactory commonNettyHttpServiceFactory) {
    this(cConf, sConf, discoveryService, discoveryServiceClient, metricsCollectionService,
        commonNettyHttpServiceFactory, null, null);
  }

  @Inject
  TaskWorkerService(CConfiguration cConf,
      SConfiguration sConf,
      DiscoveryService discoveryService,
      DiscoveryServiceClient discoveryServiceClient,
      MetricsCollectionService metricsCollectionService,
      CommonNettyHttpServiceFactory commonNettyHttpServiceFactory,
      @Nullable TokenManager tokenManager,
      @Nullable Codec<AccessToken> accessTokenCodec) {
    this.discoveryService = discoveryService;
    this.callerAuthEnabled = cConf.getBoolean(Constants.Security.INTERNAL_AUTH_ENABLED);
    Preconditions.checkState(!callerAuthEnabled || (tokenManager != null && accessTokenCodec != null),
        "TokenManager and Codec<AccessToken> are required when %s is enabled",
        Constants.Security.INTERNAL_AUTH_ENABLED);
    this.tokenManager = tokenManager;
    this.accessTokenCodec = accessTokenCodec;

    // set workdir location in cConf
    // workdir location is unique per task worker and accessible via env var
    String workDir = System.getenv("CDAP_LOCAL_DIR");
    if (workDir != null) {
      cConf.set(TaskWorker.WORK_DIR, workDir);
    }

    List<HttpHandler> handlers = Arrays.asList(
        new PingHandler(),
        new TaskWorkerHttpHandlerInternal(cConf, discoveryService,
            discoveryServiceClient, this::stopService,
            metricsCollectionService, this::isCallerAllowed));

    NettyHttpService.Builder builder = commonNettyHttpServiceFactory.builder(
            Constants.Service.TASK_WORKER, false)
        .setHost(cConf.get(Constants.TaskWorker.ADDRESS))
        .setPort(cConf.getInt(Constants.TaskWorker.PORT))
        .setExecThreadPoolSize(cConf.getInt(Constants.TaskWorker.EXEC_THREADS))
        .setBossThreadPoolSize(cConf.getInt(Constants.TaskWorker.BOSS_THREADS))
        .setWorkerThreadPoolSize(cConf.getInt(Constants.TaskWorker.WORKER_THREADS))
        .setChannelPipelineModifier(new ChannelPipelineModifier() {
          @Override
          public void modify(ChannelPipeline pipeline) {
            pipeline.addAfter("compressor", "decompressor", new HttpContentDecompressor());
          }
        })
        .setHttpHandlers(handlers);

    if (cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED)) {
      new HttpsEnabler().configureKeyStore(cConf, sConf).enable(builder);
    }
    this.httpService = builder.build();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.debug("Starting TaskWorkerService");
    if (callerAuthEnabled) {
      Services.startAndWait(tokenManager);
    }
    httpService.start();
    bindAddress = httpService.getBindAddress();
    cancelDiscovery = discoveryService.register(
        ResolvingDiscoverable.of(
            URIScheme.createDiscoverable(Constants.Service.TASK_WORKER, httpService)));
    LOG.debug("Starting TaskWorkerService has completed");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.debug("Shutting down TaskWorkerService");
    httpService.stop(1, 2, TimeUnit.SECONDS);
    cancelDiscovery.cancel();
    if (callerAuthEnabled) {
      Services.stopAndWait(tokenManager);
    }
    LOG.debug("Shutting down TaskWorkerService has completed");
  }

  private void stopService(String className) {
    /*
     * TODO: Expand this logic such that
     * based on number of requests per particular class,
     * the service gets stopped.
     */
    stopAsync();
  }

  /**
   * Returns whether a task request came from a CDAP system service, or if caller auth is disabled.
   */
  private boolean isCallerAllowed(HttpRequest request) {
    if (!callerAuthEnabled) {
      return true;
    }
    try {
      checkCaller(request.headers().get(Constants.Security.Headers.WORKER_CALLER_TOKEN));
      return true;
    } catch (Exception e) {
      LOG.warn("Rejected a task that failed the caller check: {}",
          Throwables.getRootCause(e).toString(), e);
      return false;
    }
  }

  /**
   * Checks that the caller sent an internal token of a CDAP system service.
   */
  private void checkCaller(@Nullable String callerToken)
      throws IOException, InvalidTokenException {
    String prefix = Credential.CREDENTIAL_TYPE_INTERNAL + " ";
    if (callerToken == null || !callerToken.startsWith(prefix)) {
      throw new UnauthorizedException("No internal caller token");
    }
    AccessToken accessToken = accessTokenCodec.decode(
        Base64.getDecoder().decode(callerToken.substring(prefix.length())));
    if (accessToken == null || accessToken.getIdentifier() == null) {
      throw new UnauthorizedException("Invalid internal caller token");
    }
    tokenManager.validateSecret(accessToken);
    UserIdentity identity = accessToken.getIdentifier();
    if (!UserIdentity.IdentifierType.INTERNAL.equals(identity.getIdentifierType())
        || Constants.Security.Authentication.RUNTIME_IDENTITY.equals(identity.getUsername())) {
      throw new UnauthorizedException("Caller token is not a system service internal token");
    }
  }

  @VisibleForTesting
  public InetSocketAddress getBindAddress() {
    return bindAddress;
  }
}
