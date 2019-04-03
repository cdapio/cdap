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

package co.cask.cdap.test.app;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Application that has an invalid handler in its Service.
 */
public class AppWithInvalidHandler extends AbstractApplication {

  @Override
  public void configure() {
    addService("ServiceWithInvalidHandler", new InvalidHandler());
  }

  // A handler which has an invalid value for its @Path param, due to an extra slash.
  public static final class InvalidHandler extends AbstractHttpServiceHandler {
    @Path("/write/{/key}")
    @GET
    public void handler(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("key") String key) {
      responder.sendStatus(200);
    }

  }
}
