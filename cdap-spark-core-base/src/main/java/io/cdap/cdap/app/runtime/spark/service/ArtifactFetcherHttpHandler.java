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

package io.cdap.cdap.app.runtime.spark.service;


import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.LocationBodyProducer;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.filesystem.Location;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

/**
 * HTTP handler for serving local artifacts to spark executors.
 */
@Path(Constants.Gateway.INTERNAL_API_VERSION_3 + "/artifacts")
public class ArtifactFetcherHttpHandler extends AbstractHttpHandler {
  private final Location bundleLocation;

  public ArtifactFetcherHttpHandler(Location bundleLocation) {
    this.bundleLocation = bundleLocation;
  }

  /**
   * Bundles and downloads the content of {@link this#bundleLocation}
   *
   * @param request   {@link HttpRequest}
   * @param responder {@link HttpResponse}
   */
  @GET
  @Path("/fetch")
  public void fetch(HttpRequest request, HttpResponder responder) {
    if (bundleLocation == null) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Bundle location was not found.",
                           EmptyHttpHeaders.INSTANCE);
      return;
    }
    responder.sendContent(HttpResponseStatus.OK, new LocationBodyProducer(bundleLocation),
                          new DefaultHttpHeaders().add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM));
  }

}
