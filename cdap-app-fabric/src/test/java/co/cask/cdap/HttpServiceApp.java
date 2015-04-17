/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.service.BasicService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.http.HttpResponder;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 *
 */
public class HttpServiceApp extends AbstractApplication {
  /**
   * Override this method to configure the application.
   */
  @Override
  public void configure() {
    setName("HttpServiceApp");
    setDescription("Application with Http Service");
    addService(new BasicService("HttpService", new BaseHttpHandler()));
  }

  /**
   *
   */
  @Path("/v1")
  public static final class BaseHttpHandler extends AbstractHttpServiceHandler {
    @GET
    @Path("/handle")
    public void process(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "Hello World");
    }

    @Override
    public void configure() {

    }
  }
}
