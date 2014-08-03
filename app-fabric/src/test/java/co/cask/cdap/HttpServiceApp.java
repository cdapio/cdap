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

package co.cask.cdap;

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureContext;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.procedure.ProcedureResponse;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;
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
    addService("HttpService", ImmutableList.<HttpServiceHandler>of(new BaseHttpHandler()));
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

    private ServiceDiscovered serviceDiscovered;

    /**
     *
     * @param context
     */
    @Override
    public void initialize(ProcedureContext context) {
      //Discover the UserInterestsLookup service via discovery service
      serviceDiscovered = context.discover("HttpServiceApp", "HttpService", "HttpService");
    }

    /**
     *
     * @param request
     * @param responder
     * @throws Exception
     */
    @Handle("noop")
    public void handle(ProcedureRequest request, ProcedureResponder responder) throws Exception {
      Discoverable discoverable = Iterables.getFirst(serviceDiscovered, null);
      if (discoverable != null) {
        String hostName = discoverable.getSocketAddress().getHostName();
        int port = discoverable.getSocketAddress().getPort();
        LOG.debug("host: {}, port: {}", hostName, String.valueOf(port));
        String response = doGet(hostName, port);
        LOG.debug(response);
        responder.sendJson(ProcedureResponse.Code.SUCCESS, response);
      } else {
        responder.sendJson(ProcedureResponse.Code.FAILURE, "ERROR!");
      }
    }

    /**
     *
     * @param hostName
     * @param port
     * @return
     * @throws Exception
     */
    public static String doGet(String hostName, int port) throws Exception {
      try {
        URL url = new URL(String.format("http://%s:%d/v1/handle", hostName, port));
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
