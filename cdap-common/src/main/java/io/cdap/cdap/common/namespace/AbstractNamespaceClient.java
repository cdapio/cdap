/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package io.cdap.cdap.common.namespace;

import com.google.gson.Gson;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NamespaceAlreadyExistsException;
import io.cdap.cdap.common.NamespaceCannotBeDeletedException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.NamespaceRepositoryConfig;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Common implementation of methods to interact with namespaces over HTTP.
 */
public abstract class AbstractNamespaceClient extends AbstractNamespaceQueryClient implements NamespaceAdmin {
  private static final Gson GSON = new Gson();

  @Override
  public void delete(NamespaceId namespaceId) throws Exception {
    URL url = resolve(String.format("unrecoverable/namespaces/%s", namespaceId.getNamespace()));
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
    URL url = resolve(String.format("namespaces/%s", metadata.getName()));
    HttpResponse response = execute(HttpRequest.put(url).withBody(GSON.toJson(metadata)).build());
    String responseBody = response.getResponseBodyAsString();
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      if (responseBody.equals(String.format("Namespace '%s' already exists.", metadata.getName()))) {
        throw new NamespaceAlreadyExistsException(metadata.getNamespaceId());
      }
      return;
    }
    if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException("Bad request: " + responseBody);
    }
    throw new IOException(String.format("Cannot create namespace %s. Reason: %s", metadata.getName(), responseBody));
  }

  @Override
  public void updateProperties(NamespaceId namespaceId, NamespaceMeta metadata) throws Exception {
    URL url = resolve(String.format("namespaces/%s/properties", namespaceId.getNamespace()));
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
  public void deleteDatasets(NamespaceId namespaceId) throws Exception {
    URL url = resolve(String.format("unrecoverable/namespaces/%s/datasets", namespaceId.getNamespace()));
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

  @Override
  public void updateRepository(NamespaceId namespaceId, NamespaceRepositoryConfig repository) throws Exception {
    URL url = resolve(String.format("namespaces/%s/repository", namespaceId.getNamespace()));
    HttpResponse response = execute(HttpRequest.put(url).withBody(GSON.toJson(repository)).build());
    String responseBody = response.getResponseBodyAsString();
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return;
    }
    if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException("Bad request: " + responseBody);
    }
    throw new IOException(String.format("Cannot update repository on namespace %s. Reason: %s",
                                        namespaceId, responseBody));
  }

  @Override
  public void deleteRepository(NamespaceId namespaceId) throws Exception {
    URL url = resolve(String.format("namespaces/%s/repository", namespaceId.getNamespace()));
    HttpResponse response = execute(HttpRequest.delete(url).build());
    String responseBody = response.getResponseBodyAsString();
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return;
    }
    if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException("Bad request: " + responseBody);
    }
    throw new IOException(String.format("Cannot delete repository on namespace %s. Reason: %s",
                                        namespaceId, responseBody));
  }

  public void deleteAll() throws Exception {
    for (NamespaceMeta meta : list()) {
      delete(meta.getNamespaceId());
    }
  }
}
