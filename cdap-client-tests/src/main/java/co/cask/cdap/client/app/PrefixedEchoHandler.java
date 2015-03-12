/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.client.app;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;

import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Echo handler.
 */
public final class PrefixedEchoHandler extends AbstractHttpServiceHandler {
  public static final String NAME = "echoHandler";

  private String sdf = "";

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    this.sdf = Optional.fromNullable(context.getRuntimeArguments().get("sdf")).or("");
  }

  @POST
  @Path("/echo")
  public void echo(HttpServiceRequest request, HttpServiceResponder responder) {
    String content = Bytes.toString(request.getContent());
    responder.sendString(200, sdf + ":" + content, Charsets.UTF_8);
  }
}
