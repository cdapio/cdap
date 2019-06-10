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

package io.cdap.cdap.application;

import com.google.common.reflect.TypeToken;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.UnauthenticatedException;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.cdap.common.http.ObjectResponse;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Common implementation of methods to interact with app fabric service over HTTP.
 */
public abstract class AbstractApplicationClient {

  /**
   * Executes an HTTP request.
   */
  protected abstract HttpResponse execute(HttpRequest request, int... allowedErrorCodes)
    throws IOException, UnauthenticatedException, UnauthorizedException;

  /**
   * Resolved the specified URL
   */
  protected abstract URL resolve(String resource) throws IOException;

  /**
   * Lists all applications currently deployed.
   *
   * @param namespace the namespace to list applications from
   * @return list of {@link ApplicationRecord ApplicationRecords}.
   * @throws BadRequestException if the request is invalid
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public List<ApplicationRecord> list(NamespaceId namespace)
    throws BadRequestException, IOException, UnauthenticatedException {
    String path = String.format("namespaces/%s/apps", namespace.getNamespace());
    HttpResponse response = makeRequest(path, HttpMethod.GET, null);

    return ObjectResponse.fromJsonBody(response, new TypeToken<List<ApplicationRecord>>() {
    }).getResponseObject();
  }

  /**
   * Get details about the specified application.
   *
   * @param appId the id of the application to get
   * @return details about the specified application
   * @throws ApplicationNotFoundException if the application with the given ID was not found
   * @throws BadRequestException if the request is invalid
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  public ApplicationDetail get(ApplicationId appId)
    throws ApplicationNotFoundException, BadRequestException, IOException, UnauthenticatedException {
    String path = String.format("namespaces/%s/apps/%s/versions/%s",
                                appId.getParent().getNamespace(), appId.getApplication(), appId.getVersion());
    HttpResponse response = makeRequest(path, HttpMethod.GET, null);

    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ApplicationNotFoundException(appId);
    }

    return ObjectResponse.fromJsonBody(response, ApplicationDetail.class).getResponseObject();
  }

  private HttpResponse makeRequest(String path, HttpMethod httpMethod, @Nullable String body)
    throws IOException, UnauthenticatedException, BadRequestException, UnauthorizedException {
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
