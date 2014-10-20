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
import co.cask.cdap.api.procedure.ProcedureResponse;
import co.cask.cdap.api.service.BasicService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;
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
    addProcedure(new NoOpProcedure());
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

  /**
   *
   */
  public static final class NoOpProcedure extends AbstractProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(NoOpProcedure.class);

    /**
     *
     * @param request
     * @param responder
     * @throws Exception
     */
    @Handle("noop")
    public void handle(ProcedureRequest request, ProcedureResponder responder) throws Exception {
      URL serviceURL = getContext().getServiceURL("HttpServiceApp", "HttpService");
      if (serviceURL != null) {
        String response = doGet(serviceURL);
        LOG.debug(response);
        responder.sendJson(ProcedureResponse.Code.SUCCESS, response);
      } else {
        responder.sendJson(ProcedureResponse.Code.FAILURE, "ERROR!");
      }
    }

    /**
     *
     * @param baseURL
     * @return
     * @throws Exception
     */
    public static String doGet(URL baseURL) throws Exception {
      try {
        URL url = new URL(baseURL, "v1/handle");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        if (HttpURLConnection.HTTP_OK == conn.getResponseCode()) {
          try {
            return new String(ByteStreams.toByteArray(conn.getInputStream()), Charsets.UTF_8);
          } finally {
            conn.disconnect();
          }
        }
        LOG.warn("Unexpected response from Catalog Service: {} {}", conn.getResponseCode(), conn.getResponseMessage());
      } catch (Throwable th) {
        LOG.warn("Error while callilng Catalog Service", th);
      }
      return null;
    }
  }
}
