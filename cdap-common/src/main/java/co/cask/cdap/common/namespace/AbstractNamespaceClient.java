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

import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NamespaceAlreadyExistsException;
import co.cask.cdap.common.NamespaceCannotBeCreatedException;
import co.cask.cdap.common.NamespaceCannotBeDeletedException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.proto.Id;
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
  private static final Gson GSON = new Gson();

  protected abstract HttpResponse execute(HttpRequest request) throws IOException, UnauthorizedException;
  protected abstract URL resolve(String resource) throws IOException;

  public List<NamespaceMeta> list() throws IOException, UnauthorizedException {
    HttpRequest request = HttpRequest.get(resolve("namespaces")).build();
    HttpResponse response = execute(request);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return ObjectResponse.fromJsonBody(response, new TypeToken<List<NamespaceMeta>>() { })
        .getResponseObject();
    }
    throw new IOException(String.format("Cannot list namespaces. Reason: %s", response));
  }

  public NamespaceMeta get(String namespaceId) throws NamespaceNotFoundException, IOException, UnauthorizedException {
    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    HttpResponse response = execute(HttpRequest.get(resolve(String.format("namespaces/%s", namespaceId))).build());
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NamespaceNotFoundException(namespace);
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return ObjectResponse.fromJsonBody(response, NamespaceMeta.class).getResponseObject();
    }
    throw new IOException("Cannot get namespace. Reason: " + response);
  }

  public void delete(String namespaceId)
    throws NamespaceNotFoundException, NamespaceCannotBeDeletedException, IOException, UnauthorizedException {

    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    URL url = resolve(String.format("unrecoverable/namespaces/%s", namespaceId));
    HttpResponse response = execute(HttpRequest.delete(url).build());

    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NamespaceNotFoundException(namespace);
    } else if (HttpURLConnection.HTTP_FORBIDDEN == response.getResponseCode()) {
      throw new NamespaceCannotBeDeletedException(namespace, response.getResponseBodyAsString());
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return;
    }
    throw new IOException("Cannot delete namespace. Reason: " + response);
  }

  public void create(NamespaceMeta namespaceMeta)
    throws NamespaceAlreadyExistsException, BadRequestException, IOException, UnauthorizedException,
    NamespaceCannotBeCreatedException {

    Id.Namespace namespace = Id.Namespace.from(namespaceMeta.getName());
    URL url = resolve(String.format("namespaces/%s", namespace.getId()));
    HttpResponse response = execute(HttpRequest.put(url).withBody(GSON.toJson(namespaceMeta)).build());
    String responseBody = response.getResponseBodyAsString();
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      if (responseBody != null && responseBody.equals(String.format("Namespace '%s' already exists.",
                                                                    namespaceMeta.getName()))) {
        throw new NamespaceAlreadyExistsException(namespace);
      }
      return;
    }
    if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException("Bad request: " + responseBody);
    }
    throw new IOException(String.format("Cannot create namespace %s. Reason: %s", namespaceMeta.getName(), response));
  }

  public void deleteAll() throws
    IOException, UnauthorizedException, NamespaceNotFoundException, NamespaceCannotBeDeletedException {
    for (NamespaceMeta meta : list()) {
      delete(meta.getName());
    }
  }
}
