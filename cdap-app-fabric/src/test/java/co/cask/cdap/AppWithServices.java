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

package co.cask.cdap;

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.service.BasicService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;

import java.io.IOException;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Test Application with services for the new application API.
 */
public class AppWithServices extends AbstractApplication {
  /**
   * Override this method to configure the application.
   */
  @Override
  public void configure() {
    setName("AppWithServices");
    setDescription("Application with Services");
    addProcedure(new NoOpProcedure());
    addService(new BasicService("NoOpService", new PingHandler(), new MultiPingHandler()));
  }

  public static final class PingHandler extends AbstractHttpServiceHandler {

    private HttpServiceContext context;

    @Override
    public void initialize(HttpServiceContext context) {
      this.context = context;
      context.setNote("call", Integer.toString(0));
    }

    @Path("ping")
    @GET
    public void handler(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendStatus(200);
    }

    @Path("count")
    @POST
    public void incrCount(HttpServiceRequest request, HttpServiceResponder responder) {
      context.setNote("call", Integer.toString(Integer.valueOf(context.getNote("call")) + 1));
      responder.sendStatus(200);
    }

    @Path("count")
    @GET
    public void getCount(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendString(200, context.getNote("call"), Charsets.UTF_8);
    }
  }

  @Path("/multi")
  public static final class MultiPingHandler extends AbstractHttpServiceHandler {

    // No method level path, multiple request types.
    @POST
    @GET
    public void handler(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendStatus(200);
    }

    @GET
    @Path("/ping")
    public void pingHandler(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendStatus(200);
    }
  }

  public static final class NoOpProcedure extends AbstractProcedure {
    @Handle("noop")
    public void handle(ProcedureRequest request, ProcedureResponder responder) throws IOException {
      responder.sendJson("OK");
    }
  }
}
