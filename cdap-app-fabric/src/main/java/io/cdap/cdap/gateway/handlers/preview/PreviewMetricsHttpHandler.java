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

package io.cdap.cdap.gateway.handlers.preview;


import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.app.preview.PreviewManager;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Internal {@link HttpHandler} for Preview system.
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_K8S_METRICS)
public class PreviewMetricsHttpHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(PreviewHttpHandlerInternal.class);
  private static final Gson GSON = new Gson();
  private final PreviewManager previewManager;

  @Inject
  public PreviewMetricsHttpHandler(PreviewManager previewManager) {
    this.previewManager = previewManager;
  }
  @GET
  @Path("/")
  public void getTracers(HttpRequest request, HttpResponder responder) {
    responder.sendString(HttpResponseStatus.OK, "healthy");
  }

  @GET
  @Path("/namespaces/default/services/cdap-a1008e-preview/queueLength")
  public void getQueueLength(FullHttpRequest request, HttpResponder responder) {
  }
}
