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
 */

package io.cdap.cdap.internal.app.worker.sidecar;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.File;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Internal {@link HttpHandler} for File Localizer.
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3 + "/worker")
public class ArtifactLocalizerHttpHandlerInternal extends AbstractHttpHandler {
  private final ArtifactLocalizer artifactLocalizer;

  @Inject
  public ArtifactLocalizerHttpHandlerInternal(ArtifactLocalizer artifactLocalizer) {
    this.artifactLocalizer = artifactLocalizer;
  }

  @GET
  @Path("/artifact/namespaces/{namespace-id}/artifacts/{artifact-name}/versions/{artifact-version}")
  public void artifact(HttpRequest request, HttpResponder responder,
                       @PathParam("namespace-id") String namespaceId,
                       @PathParam("artifact-name") String artifactName,
                       @PathParam("artifact-version") String artifactVersion,
                       @QueryParam("unpack") @DefaultValue("false") String unpack) throws Exception {
    File artifactPath;
    ArtifactId artifactId = new ArtifactId(namespaceId, artifactName, artifactVersion);
    if (Boolean.parseBoolean(unpack)) {
      artifactPath = artifactLocalizer.getAndUnpackArtifact(artifactId);
    } else {
      artifactPath = artifactLocalizer.getArtifact(artifactId);
    }
    responder.sendString(HttpResponseStatus.OK, artifactPath.toString());
  }
}
