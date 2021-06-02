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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.logging.gateway.handlers.AbstractLogHttpHandler;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Internal {@link HttpHandler} for File Localizer.
 */
@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3 + "/worker")
public class FileLocalizerHttpHandlerInternal extends AbstractLogHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(FileLocalizerHttpHandlerInternal.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(BasicThrowable.class, new BasicThrowableCodec()).create();

  @Inject
  public FileLocalizerHttpHandlerInternal(CConfiguration cConf) {
    super(cConf);
  }

  @GET
  @Path("/localize")
  public void run(FullHttpRequest request, HttpResponder responder) {

  }

  /**
   * Return json representation of an exception.
   * Used to propagate exception across network for better surfacing errors and debuggability.
   */
  private String exceptionToJson(Exception ex) {
    BasicThrowable basicThrowable = new BasicThrowable(ex);
    return GSON.toJson(basicThrowable);
  }
}
