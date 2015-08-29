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

package co.cask.cdap.metadata.service;

import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.DatasetNotFoundException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.StreamNotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.Id;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Singleton;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * HttpHandler for Metadata
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3)
public class MetadataHttpHandler extends AbstractHttpHandler {
  private static final String NOT_FOUND = "notfound";
  private static final String EMPTY = "empty";

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/metadata")
  public void getAppMetadata(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("app-id") String app) throws NotFoundException {
    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    if (NOT_FOUND.equals(namespaceId)) {
      throw new NamespaceNotFoundException(namespace);
    }
    if (NOT_FOUND.equals(app)) {
      throw new ApplicationNotFoundException(Id.Application.from(namespace, app));
    }
    Map<String, String> metadata;
    if (EMPTY.equals(namespaceId) || EMPTY.equals(app)) {
      metadata = ImmutableMap.of();
    } else {
      metadata = ImmutableMap.of("aKey", "aValue",
                                 "aK", "aV",
                                 "aK1", "aV1",
                                 "tags", "counter,mr,visual");
    }
    responder.sendJson(HttpResponseStatus.OK, metadata);
  }

  @GET
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/metadata")
  public void getDatasetMetadata(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("dataset-id") String datasetId) throws NotFoundException {
    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    if (NOT_FOUND.equals(namespaceId)) {
      throw new NamespaceNotFoundException(namespace);
    }
    if (NOT_FOUND.equals(datasetId)) {
      throw new DatasetNotFoundException(Id.DatasetInstance.from(namespace, datasetId));
    }
    Map<String, String> metadata;
    if (EMPTY.equals(namespaceId) || EMPTY.equals(datasetId)) {
      metadata = ImmutableMap.of();
    } else {
      metadata = ImmutableMap.of("dKey", "dValue",
                                 "dK", "dV",
                                 "dK1", "dV1",
                                 "tags", "reports,deviations,errors");
    }
    responder.sendJson(HttpResponseStatus.OK, metadata);
  }

  @GET
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/metadata")
  public void getStreamMetadata(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("stream-id") String streamId) throws NotFoundException {
    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    if (NOT_FOUND.equals(namespaceId)) {
      throw new NamespaceNotFoundException(namespace);
    }
    if (NOT_FOUND.equals(streamId)) {
      throw new StreamNotFoundException(Id.Stream.from(namespace, streamId));
    }
    Map<String, String> metadata;
    if (EMPTY.equals(namespaceId) || EMPTY.equals(streamId)) {
      metadata = ImmutableMap.of();
    } else {
      metadata = ImmutableMap.of("sKey", "sValue",
                                 "sK", "sV",
                                 "sK1", "sV1",
                                 "tags", "input,raw");
    }
    responder.sendJson(HttpResponseStatus.OK, metadata);
  }

  @POST
  @Path("/metadata/history")
  public void recordRun(HttpRequest request, HttpResponder responder) {
    responder.sendString(HttpResponseStatus.OK, "Metadata recorded successfully");
  }
}
