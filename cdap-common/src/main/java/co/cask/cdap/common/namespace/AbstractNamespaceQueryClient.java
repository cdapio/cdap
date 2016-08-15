/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.common.namespace;

import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

/**
 * Implementation of {@link NamespaceQueryAdmin} that fetchs {@link NamespaceMeta} through REST API.
 */
public abstract class AbstractNamespaceQueryClient implements NamespaceQueryAdmin {

  /**
   * Executes an HTTP request.
   */
  protected abstract HttpResponse execute(HttpRequest request)
    throws IOException, UnauthenticatedException, UnauthorizedException;

  /**
   * Resolves the specified URL.
   */
  protected abstract URL resolve(String resource) throws IOException;

  @Override
  public List<NamespaceMeta> list() throws Exception {
    HttpRequest request = HttpRequest.get(resolve("namespaces")).build();
    HttpResponse response = execute(request);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return ObjectResponse.fromJsonBody(response, new TypeToken<List<NamespaceMeta>>() { }).getResponseObject();
    }
    throw new IOException(String.format("Cannot list namespaces. Reason: %s", response.getResponseBodyAsString()));
  }

  @Override
  public NamespaceMeta get(Id.Namespace namespaceId) throws Exception {
    HttpResponse response =
      execute(HttpRequest.get(resolve(String.format("namespaces/%s", namespaceId.getId()))).build());
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NamespaceNotFoundException(namespaceId);
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return ObjectResponse.fromJsonBody(response, NamespaceMeta.class).getResponseObject();
    }
    throw new IOException(String.format("Cannot get namespace %s. Reason: %s",
                                        namespaceId, response.getResponseBodyAsString()));
  }

  @Override
  public boolean exists(Id.Namespace namespaceId) throws Exception {
    try {
      get(namespaceId);
    } catch (NamespaceNotFoundException e) {
      return false;
    }
    return true;
  }
}
