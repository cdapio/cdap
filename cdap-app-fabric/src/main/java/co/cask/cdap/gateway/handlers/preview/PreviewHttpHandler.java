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

package co.cask.cdap.gateway.handlers.preview;

import co.cask.cdap.app.preview.PreviewManager;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.codec.BasicThrowableCodec;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.InputStreamReader;
import java.io.Reader;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * {@link co.cask.http.HttpHandler} to manage preview lifecycle for v3 REST APIs
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class PreviewHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(BasicThrowable.class, new BasicThrowableCodec()).create();

  private final PreviewManager previewManager;

  @Inject
  PreviewHttpHandler(PreviewManager previewManager) {
    this.previewManager = previewManager;
  }

  @POST
  @Path("/previews")
  public void start(HttpRequest request, HttpResponder responder,
                    @PathParam("namespace-id") String namespaceId) throws Exception {
    NamespaceId namespace = new NamespaceId(namespaceId);
    AppRequest appRequest;
    try (Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()), Charsets.UTF_8)) {
      appRequest = GSON.fromJson(reader, AppRequest.class);
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Request body is invalid json: " + e.getMessage());
    }
    responder.sendString(HttpResponseStatus.OK, GSON.toJson(previewManager.start(namespace, appRequest)));
  }

  @POST
  @Path("/previews/{preview-id}/stop")
  public void stop(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                   @PathParam("preview-id") String previewId) throws Exception {
    NamespaceId namespace = new NamespaceId(namespaceId);
    ApplicationId application = namespace.app(previewId);
    previewManager.getRunner(application).stopPreview();
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @GET
  @Path("/previews/{preview-id}/status")
  public void getStatus(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                        @PathParam("preview-id") String previewId)  throws Exception {
    NamespaceId namespace = new NamespaceId(namespaceId);
    ApplicationId application = namespace.app(previewId);
    responder.sendString(HttpResponseStatus.OK, GSON.toJson(previewManager.getRunner(application).getStatus()));
  }

  @GET
  @Path("/previews/{preview-id}/tracers")
  public void getTracers(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                         @PathParam("preview-id") String previewId) throws Exception {
    // TODO Implement API in PreviewStore to get all the tracers.
  }

  @GET
  @Path("/previews/{preview-id}/tracers/{tracer-id}")
  public void getData(HttpRequest request, HttpResponder responder,
                      @PathParam("namespace-id") String namespaceId,
                      @PathParam("preview-id") String previewId,
                      @PathParam("tracer-id") String tracerId) throws Exception {
    NamespaceId namespace = new NamespaceId(namespaceId);
    ApplicationId application = namespace.app(previewId);
    responder.sendString(HttpResponseStatus.OK, GSON.toJson(previewManager.getRunner(application).getData(tracerId)));
  }
}
