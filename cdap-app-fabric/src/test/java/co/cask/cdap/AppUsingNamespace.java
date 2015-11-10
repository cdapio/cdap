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

package co.cask.cdap;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * App used to test namespace availability.
 */
public class AppUsingNamespace extends AbstractApplication {
  public static final String SERVICE_NAME = "nsService";

  @Override
  public void configure() {
    addService(new NamespaceService());
  }

  public static class NamespaceService extends AbstractService {
    @Override
    protected void configure() {
      setName(SERVICE_NAME);
      addHandler(new NamespaceHandler());
    }
  }

  public static class NamespaceHandler extends AbstractHttpServiceHandler {
    private String namespace;

    @Override
    public void initialize(HttpServiceContext context) throws Exception {
      super.initialize(context);
      namespace = context.getNamespace();
    }

    @Path("/ns")
    @GET
    public void getNamespace(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendString(namespace);
    }
  }
}
