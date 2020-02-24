/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.store.ApplicationMeta;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;


/**
 * {@link io.cdap.http.HttpHandler} to get application metadata for v3 REST APIs
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class ApplicationMetadataHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Gson GSON =
    ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
  private static final Type LIST_APPLICATION_META_TYPE = new TypeToken<List<ApplicationMeta>>() { }.getType();

  /**
   * store manages non-runtime lifecycle.
   */
  protected final Store store;

  @Inject
  ApplicationMetadataHttpHandler(Store store) {
    this.store = store;
  }

  @GET
  @Path("/apps/metadata")
  public void getAllApplicationMetadata(HttpRequest request, HttpResponder responder,
                                        @PathParam("namespace-id") String namespaceId) {
    NamespaceId id = new NamespaceId(namespaceId);
    Collection<ApplicationMeta> allMetadata = store.getAllApplicationMetadata(id);

    List<ApplicationMeta> metaList = allMetadata.stream().collect(Collectors.toList());
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(metaList, LIST_APPLICATION_META_TYPE));
  }

  @GET
  @Path("/apps/{app-id}/versions/{version-id}/metadata")
  public void getApplicationMetadata(HttpRequest request, HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId,
                                     @PathParam("app-id") String appId,
                                     @PathParam("version-id") String versionId) throws ApplicationNotFoundException {
    ApplicationId applicationId = new ApplicationId(namespaceId, appId, versionId);
    ApplicationMeta appMeta = store.getApplicationMetadata(applicationId);
    if (appMeta == null) {
      throw new ApplicationNotFoundException(applicationId);
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(appMeta));
  }
}
