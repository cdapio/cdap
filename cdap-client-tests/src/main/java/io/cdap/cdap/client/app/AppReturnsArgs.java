/*
 * Copyright © 2015 Cask Data, Inc.
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

package io.cdap.cdap.client.app;

import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.service.BasicService;
import io.cdap.cdap.api.service.http.AbstractHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Test Application with service which returns the runtime arguments.
 */
public class AppReturnsArgs extends AbstractApplication {

  public static final String NAME = "AppReturnsArgs";
  public static final String SERVICE = "ArgService";
  public static final String ENDPOINT = "args";

  @Override
  public void configure() {
    setName(NAME);
    setDescription("Application with Service which returns Runtime Arguments");
    addService(new BasicService(SERVICE, new ArgService()));
  }

  /**
   *
   */
  public static final class ArgService extends AbstractHttpServiceHandler {

    @Path(ENDPOINT)
    @GET
    public void handler(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendJson(200, getContext().getRuntimeArguments());
    }
  }
}
