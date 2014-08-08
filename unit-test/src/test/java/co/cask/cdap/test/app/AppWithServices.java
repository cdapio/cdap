/*
 * Copyright 2014 Cask, Inc.
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

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.procedure.ProcedureResponse;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.ElectionHandler;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.common.Cancellable;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * AppWithServices with a DummyService for unit testing.
 */
public class AppWithServices extends AbstractApplication {
  public static final String SERVICE_NAME = "ServerService";

    @Override
    public void configure() {
      setName("AppWithServices");
      addStream(new Stream("text"));
      addProcedure(new NoOpProcedure());
      addService(SERVICE_NAME, new ServerService());
   }


  public static final class NoOpProcedure extends AbstractProcedure {

    @Handle("ping")
    public void handle(ProcedureRequest request, ProcedureResponder responder) throws IOException {
      responder.sendJson(ProcedureResponse.Code.SUCCESS, "OK");
    }

  }

  @Path("/")
  public class ServerService extends AbstractHttpServiceHandler {

    @Path("/ping2")
    @GET
    public void handler(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendStatus(200);
    }
  }
}
