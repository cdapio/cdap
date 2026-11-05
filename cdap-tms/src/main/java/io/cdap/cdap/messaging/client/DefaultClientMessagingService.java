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

package io.cdap.cdap.messaging.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import java.io.IOException;
import java.net.HttpURLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The client implementation of {@link MessagingService} that uses Messaging service endpoint.
 */
public class DefaultClientMessagingService extends AbstractClientMessagingService {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultClientMessagingService.class);

  /**
   * Creates a {@link DefaultClientMessagingService} targeting {@link Constants.Service#MESSAGING_SERVICE}.
   */
  @Inject
  public DefaultClientMessagingService(CConfiguration cConf,
      RemoteClientFactory remoteClientFactory) {
    super(remoteClientFactory.createRemoteClient(Constants.Service.MESSAGING_SERVICE,
            HTTP_REQUEST_CONFIG, "/v1/namespaces/"),
        cConf.getBoolean(Constants.MessagingSystem.HTTP_COMPRESS_PAYLOAD));
    LOG.info("DefaultClientMessagingService initialised.");
  }

  @VisibleForTesting
  public DefaultClientMessagingService(RemoteClientFactory remoteClientFactory,
      boolean compressPayload) {
    super(remoteClientFactory.createRemoteClient(Constants.Service.MESSAGING_SERVICE,
        HTTP_REQUEST_CONFIG, "/v1/namespaces/"), compressPayload);
  }

  /**
   * Executes the request with {@link SecurityRequestContext} user details cleared so that
   * service-to-service calls to the internal {@code /v1} messaging endpoints authenticate as the
   * calling service rather than forwarding an end-user credential from an enclosing {@code /v3}
   * HTTP request thread.
   */
  @Override
  protected HttpResponse execute(HttpRequest request) throws IOException, UnauthorizedException {
    return executeWithoutUserDetails(() -> super.execute(request));
  }

  @Override
  protected HttpURLConnection openConnection(HttpMethod method, String resource)
      throws IOException {
    return executeWithoutUserDetails(() -> super.openConnection(method, resource));
  }

  private static <T> T executeWithoutUserDetails(Retries.Callable<T, IOException> callable)
      throws IOException {
    String oldUserId = SecurityRequestContext.getUserId();
    Credential oldUserCredential = SecurityRequestContext.getUserCredential();
    String oldUserIp = SecurityRequestContext.getUserIp();
    SecurityRequestContext.resetUserDetails();
    try {
      return callable.call();
    } finally {
      SecurityRequestContext.resetUserDetails();
      if (oldUserId != null) {
        SecurityRequestContext.setUserId(oldUserId);
      }
      if (oldUserCredential != null) {
        SecurityRequestContext.setUserCredential(oldUserCredential);
      }
      if (oldUserIp != null) {
        SecurityRequestContext.setUserIp(oldUserIp);
      }
    }
  }
}
