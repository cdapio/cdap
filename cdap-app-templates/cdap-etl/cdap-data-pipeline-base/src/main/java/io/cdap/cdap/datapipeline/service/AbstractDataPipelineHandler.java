/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.datapipeline.service;

import com.google.gson.JsonSyntaxException;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.service.http.AbstractSystemHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.etl.proto.CodedException;
import io.cdap.cdap.proto.id.NamespaceId;

import java.io.IOException;
import java.net.HttpURLConnection;

/**
 * Common functionality for data pipeline services.
 */
public class AbstractDataPipelineHandler extends AbstractSystemHttpServiceHandler {

  /**
   * Utility method for executing an endpoint with common error handling and namespace checks built in.
   *
   * If the callable throws a {@link StatusCodeException}, the exception's status code and message will be used
   * to create the response.
   * If a {@link JsonSyntaxException} is thrown, a 400 response will be sent.
   * If anything else if thrown, a 500 response will be sent.
   *
   * @param responder the http responder
   * @param namespace the namespace to check for
   * @param callable the endpoint logic to run
   */
  protected <T> void respond(String namespace, HttpServiceResponder responder, NamespacedResponder callable) {
    // system namespace does not officially exist, so don't check existence for system namespace.
    NamespaceSummary namespaceSummary;
    if (NamespaceId.SYSTEM.getNamespace().equals(namespace.toLowerCase())) {
      namespaceSummary = new NamespaceSummary(NamespaceId.SYSTEM.getNamespace(), "", 0L);
    } else {
      try {
        namespaceSummary = getContext().getAdmin().getNamespaceSummary(namespace);
        if (namespaceSummary == null) {
          responder.sendJson(HttpURLConnection.HTTP_NOT_FOUND,
                             String.format("Namespace '%s' does not exist", namespace));
          return;
        }
      } catch (IOException e) {
        responder.sendJson(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
        return;
      }
    }

    try {
      callable.respond(namespaceSummary);
    } catch (CodedException e) {
      responder.sendError(e.getCode(), e.getMessage());
    } catch (IllegalArgumentException e) {
      responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
    } catch (JsonSyntaxException e) {
      responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Unable to decode request body: " + e.getMessage());
    } catch (Throwable t) {
      responder.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, t.getMessage());
    }
  }

  /**
   * Responds to a request within a namespace.
   */
  protected interface NamespacedResponder {

    /**
     * Create the response that should be returned by the endpoint.
     */
    void respond(NamespaceSummary namespace) throws Exception;
  }
}
