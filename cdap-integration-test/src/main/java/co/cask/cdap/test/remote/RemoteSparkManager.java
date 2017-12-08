/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.test.remote;

import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.common.service.ServiceDiscoverable;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.test.AbstractProgramManager;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.SparkManager;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Implementation of {@link SparkManager} that interacts with CDAP using REST API.
 */
public class RemoteSparkManager extends AbstractProgramManager<SparkManager> implements SparkManager {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteSparkManager.class);

  private final ClientConfig clientConfig;
  private final RESTClient restClient;

  public RemoteSparkManager(ProgramId programId, ApplicationManager applicationManager,
                            ClientConfig clientConfig, RESTClient restClient) {
    super(programId, applicationManager);
    this.clientConfig = clientConfig;
    this.restClient = restClient;
  }

  @Override
  public URL getServiceURL() {
    return getServiceURL(30, TimeUnit.SECONDS);
  }

  @Override
  public URL getServiceURL(long timeout, TimeUnit timeoutUnit) {
    try {
      Tasks.waitFor(true, () -> {
        try {
          checkAvailability();
          return true;
        } catch (ServiceUnavailableException e) {
          return false;
        }
      }, timeout, timeoutUnit);

      ConnectionConfig connectionConfig = clientConfig.getConnectionConfig();
      return ServiceDiscoverable.createServiceBaseURL(connectionConfig.getHostname(), connectionConfig.getPort(),
                                                      connectionConfig.isSSLEnabled(), programId);
    } catch (TimeoutException e) {
      return null;
    } catch (Exception e) {
      LOG.warn("Exception raised when waiting for Spark service to be available", e);
      return null;
    }
  }

  /**
   * Checks if a user service is available by hitting the availability endpoint.
   */
  private void checkAvailability() throws IOException, UnauthenticatedException, NotFoundException {
    URL url = clientConfig.resolveNamespacedURLV3(programId.getNamespaceId(),
                                                  String.format("apps/%s/versions/%s/%s/%s/available",
                                                                programId.getApplication(), programId.getVersion(),
                                                                programId.getType().getCategoryName(),
                                                                programId.getProgram()));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, clientConfig.getAccessToken(),
                                               HttpURLConnection.HTTP_NOT_FOUND, HttpURLConnection.HTTP_BAD_REQUEST,
                                               HttpURLConnection.HTTP_UNAVAILABLE);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(programId);
    }

    if (response.getResponseCode() == HttpURLConnection.HTTP_UNAVAILABLE) {
      throw new ServiceUnavailableException(programId.toString());
    }
  }
}
