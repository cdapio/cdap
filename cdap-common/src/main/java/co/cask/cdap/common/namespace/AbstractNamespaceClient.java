/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.common.exception.AlreadyExistsException;
import co.cask.cdap.common.exception.BadRequestException;
import co.cask.cdap.common.exception.CannotBeDeletedException;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.common.exception.UnauthorizedException;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

/**
 * Common implementation of methods to interact with namespace service.
 */
public abstract class AbstractNamespaceClient {
  private static final String NAMESPACE_ENTITY_TYPE = "namespace";

  protected abstract HttpResponse execute(HttpRequest request) throws IOException, UnauthorizedException;
  protected abstract URL resolve(String resource) throws IOException;

  public List<NamespaceMeta> list() throws IOException, UnauthorizedException {
    HttpRequest request = HttpRequest.get(resolve("namespaces")).build();
    HttpResponse response = execute(request);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return ObjectResponse.fromJsonBody(response, new TypeToken<List<NamespaceMeta>>() { })
        .getResponseObject();
    }
    throw new IOException("Cannot list namespaces. Reason: " + "getDetails(response)");
  }

  public NamespaceMeta get(String namespaceId) throws NotFoundException, IOException, UnauthorizedException {
    HttpResponse response = execute(HttpRequest.get(resolve(String.format("namespaces/%s", namespaceId))).build());
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(NAMESPACE_ENTITY_TYPE, namespaceId);
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return ObjectResponse.fromJsonBody(response, NamespaceMeta.class).getResponseObject();
    }
    throw new IOException("Cannot get namespace. Reason: " + "getDetails(response)");
  }

  public void delete(String namespaceId) throws NotFoundException, CannotBeDeletedException, IOException,
    UnauthorizedException {
    URL url = resolve(String.format("unrecoverable/namespaces/%s", namespaceId));
    HttpResponse response = execute(HttpRequest.delete(url).build());
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(NAMESPACE_ENTITY_TYPE, namespaceId);
    } else if (HttpURLConnection.HTTP_FORBIDDEN == response.getResponseCode()) {
      throw new CannotBeDeletedException(NAMESPACE_ENTITY_TYPE, namespaceId);
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return;
    }
    throw new IOException("Cannot delete namespace. Reason: " + "getDetails(response)");
  }

  public void create(NamespaceMeta namespaceMeta) throws AlreadyExistsException, BadRequestException, IOException,
    UnauthorizedException {
    URL url = resolve(String.format("namespaces/%s", namespaceMeta.getId()));
    HttpResponse response = execute(HttpRequest.put(url).withBody(new Gson().toJson(namespaceMeta)).build());
    String responseBody = response.getResponseBodyAsString();
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      if (responseBody != null && responseBody.equals(String.format("Namespace '%s' already exists.",
                                                                    namespaceMeta.getId()))) {
        throw new AlreadyExistsException(NAMESPACE_ENTITY_TYPE, namespaceMeta.getId());
      }
      return;
    }
    if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException("Bad request: " + responseBody);
    }
    throw new IOException("Cannot get create namespace. Reason: " + "getDetails(response)");
  }
}
