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

import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.app.preview.PreviewManager;
import io.cdap.cdap.app.preview.PreviewRequest;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Optional;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Internal {@link HttpHandler} for Preview system.
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3 + "/previews")
public class PreviewHttpHandlerInternal extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(PreviewHttpHandlerInternal.class);
  private static final Gson GSON = new Gson();
  private final PreviewManager previewManager;

  @Inject
  PreviewHttpHandlerInternal(PreviewManager previewManager) {
    this.previewManager = previewManager;
  }

  @POST
  @Path("/poll")
  public void poll(FullHttpRequest request, HttpResponder responder) throws Exception {
    try (InputStream is = new ByteBufInputStream(request.content())) {
      byte[] pollerInfo = ByteStreams.toByteArray(is);
      Optional<PreviewRequest> previewRequestOptional = previewManager.poll(pollerInfo);
      if (previewRequestOptional.isPresent()) {
        LOG.info("Received poller info is {}", Bytes.toString(pollerInfo));
        responder.sendString(HttpResponseStatus.OK, GSON.toJson(previewRequestOptional.get()));
      } else {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      }
    }
  }
}
