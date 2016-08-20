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

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.app.preview.PreviewManager;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.PreviewId;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * {@link co.cask.http.HttpHandler} to manage program lifecycle for v3 REST APIs
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class PreviewHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(Schema.class,
                                                                         new SchemaTypeAdapter()).create();
  private final PreviewManager previewManager;
  private final int limit;

  @Inject
  PreviewHttpHandler(CConfiguration cConf, PreviewManager previewManager) {
    this.previewManager = previewManager;
    limit = cConf.getInt("preview.records.limit", 50);
  }

  @POST
  @Path("/preview")
  public void startPreview(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId) throws Exception {
    String config = request.getContent().toString(Charsets.UTF_8);
    responder.sendString(HttpResponseStatus.OK,
                         GSON.toJson(previewManager.start(new NamespaceId(namespaceId), config)));
  }

  @GET
  @Path("/previews/{preview-id}/status")
  public void getPreviewStatus(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("preview-id") String previewId) throws NotFoundException {
    responder.sendString(HttpResponseStatus.OK,
                         GSON.toJson(previewManager.getStatus(new PreviewId(namespaceId, previewId))));
  }

  @GET
  @Path("/previews/{preview-id}/stages/{stage-name}")
  public void getPreviewData(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("preview-id") String previewId,
                             @PathParam("stage-name") String stageName) throws NotFoundException {
    Map<String, List<String>> data = previewManager.getData(new PreviewId(namespaceId, previewId), stageName);
    Map<String, List<String>> result = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : data.entrySet()) {
      String stage = entry.getKey();
      List<String> stageData = entry.getValue();
      List<String> values = stageData.size() <= limit ? stageData : stageData.subList(0, limit);
      result.put(stage, values);
    }
    responder.sendString(HttpResponseStatus.OK, GSON.toJson(result));
  }
}
