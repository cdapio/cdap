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
import co.cask.cdap.common.NamespaceCannotBeDeletedException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.UnauthenticatedException;
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
 * Common implementation of methods to interact with namespaces over HTTP.
 */
public abstract class AbstractNamespaceClient implements NamespaceAdmin {
  private static final Gson GSON = new Gson();

  /**
   * Executes an HTTP request.
   */
  protected abstract HttpResponse execute(HttpRequest request) throws IOException, UnauthenticatedException;

  /**
   * Resolves the specified URL.
   */
  protected abstract URL resolve(String resource) throws IOException;

  @Override
  public List<NamespaceMeta> list() throws Exception {
    HttpRequest request = HttpRequest.get(resolve("namespaces")).build();
    HttpResponse response = execute(request);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return ObjectResponse.fromJsonBody(response, new TypeToken<List<NamespaceMeta>>() { })
        .getResponseObject();
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
  public void delete(Id.Namespace namespaceId) throws Exception {
    URL url = resolve(String.format("unrecoverable/namespaces/%s", namespaceId.getId()));
    HttpResponse response = execute(HttpRequest.delete(url).build());

    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NamespaceNotFoundException(namespaceId);
    } else if (HttpURLConnection.HTTP_FORBIDDEN == response.getResponseCode()) {
      throw new NamespaceCannotBeDeletedException(namespaceId, response.getResponseBodyAsString());
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return;
    }
    throw new IOException(String.format("Cannot delete namespace %s. Reason: %s",
                                        namespaceId, response.getResponseBodyAsString()));
  }

  @Override
  public void create(NamespaceMeta metadata) throws Exception {
    Id.Namespace namespace = Id.Namespace.from(metadata.getName());
    URL url = resolve(String.format("namespaces/%s", namespace.getId()));
    HttpResponse response = execute(HttpRequest.put(url).withBody(GSON.toJson(metadata)).build());
    String responseBody = response.getResponseBodyAsString();
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      if (responseBody.equals(String.format("Namespace '%s' already exists.", metadata.getName()))) {
        throw new NamespaceAlreadyExistsException(namespace);
      }
      return;
    }
    if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException("Bad request: " + responseBody);
    }
    throw new IOException(String.format("Cannot create namespace %s. Reason: %s", metadata.getName(), responseBody));
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

  @Override
  public void updateProperties(Id.Namespace namespaceId, NamespaceMeta metadata) throws Exception {
    URL url = resolve(String.format("namespaces/%s/properties", namespaceId.getId()));
    HttpResponse response = execute(HttpRequest.put(url).withBody(GSON.toJson(metadata)).build());
    String responseBody = response.getResponseBodyAsString();
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return;
    }
    if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException("Bad request: " + responseBody);
    }
    throw new IOException(String.format("Cannot update namespace %s. Reason: %s", namespaceId, responseBody));
  }

  @Override
  public void deleteDatasets(Id.Namespace namespaceId) throws Exception {
    URL url = resolve(String.format("unrecoverable/namespaces/%s/datasets", namespaceId.getId()));
    HttpResponse response = execute(HttpRequest.delete(url).build());

    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NamespaceNotFoundException(namespaceId);
    } else if (HttpURLConnection.HTTP_FORBIDDEN == response.getResponseCode()) {
      String msg = String.format("Datasets in the namespace '%s' cannot be deleted. Reason: '%s'.", namespaceId,
                                 response.getResponseBodyAsString());
      throw new NamespaceCannotBeDeletedException(namespaceId, msg);
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return;
    }
    throw new IOException(String.format("Cannot delete datasets in namespace %s. Reason: %s",
                                        namespaceId, response.getResponseBodyAsString()));
  }

  public void deleteAll() throws Exception {
    for (NamespaceMeta meta : list()) {
      delete(Id.Namespace.from(meta.getName()));
    }
  }
}
