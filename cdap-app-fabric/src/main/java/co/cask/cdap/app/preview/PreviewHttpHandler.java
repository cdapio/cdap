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

package co.cask.cdap.app.preview;

import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.proto.Id;
import co.cask.http.HttpResponder;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.inject.Singleton;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.HashSet;
import java.util.Set;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * {@link co.cask.http.HttpHandler} to manage program lifecycle for v3 REST APIs
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class PreviewHttpHandler extends AbstractAppFabricHttpHandler {
  /**
   * Returns a list of applications associated with a namespace.
   */
  @GET
  @Path("/previews/{preview-id}")
  public void getPreview(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace-id") String namespaceId,
                         @QueryParam("preview-id") String previewId)
    throws NamespaceNotFoundException, BadRequestException {

    responder.sendJson(HttpResponseStatus.OK, String.format("Welcome to preview"));
  }
}
