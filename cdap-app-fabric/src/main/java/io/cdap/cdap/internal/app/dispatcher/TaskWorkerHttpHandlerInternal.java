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

package io.cdap.cdap.internal.app.dispatcher;

import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.logging.gateway.handlers.AbstractLogHttpHandler;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Internal {@link HttpHandler} for Preview system.
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3 + "/worker")
public class TaskWorkerHttpHandlerInternal extends AbstractLogHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TaskWorkerHttpHandlerInternal.class);
  private static final Gson GSON = new Gson();

  @Inject
  TaskWorkerHttpHandlerInternal(CConfiguration cConf) {
    super(cConf);
  }

  @POST
  @Path("/echo")
  public void run(FullHttpRequest request, HttpResponder responder) {
    responder.sendString(HttpResponseStatus.OK, request.content().toString(StandardCharsets.UTF_8));
  }

  @GET
  @Path("/get")
  public void get(FullHttpRequest request, HttpResponder responder) throws Exception {
    responder.sendString(HttpResponseStatus.OK, "Get succeeded");
  }
}
