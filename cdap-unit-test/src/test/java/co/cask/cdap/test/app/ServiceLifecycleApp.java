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

package co.cask.cdap.test.app;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpContentConsumer;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.common.utils.ImmutablePair;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

/**
 * An application for testing service lifecycle by the {@link ServiceLifeCycleTestRun}.
 */
public class ServiceLifecycleApp extends AbstractApplication {

  @Override
  public void configure() {
    addService("test", new TestHandler());
  }

  /**
   * Handler for testing that tracks lifecycle method calls through a static list.
   */
  public static final class TestHandler extends AbstractHttpServiceHandler {

    private static final Queue<ImmutablePair<Integer, String>> STATES = new ConcurrentLinkedQueue<>();

    @Override
    public void initialize(HttpServiceContext context) throws Exception {
      super.initialize(context);
      STATES.add(ImmutablePair.of(System.identityHashCode(this), "INIT"));
    }

    @GET
    @Path("/states")
    public void getStates(HttpServiceRequest request, HttpServiceResponder responder) {
      // Returns the current states
      responder.sendJson(new ArrayList<>(STATES));
    }

    @PUT
    @Path("/upload")
    public HttpContentConsumer upload(HttpServiceRequest request, HttpServiceResponder responder) {
      return new HttpContentConsumer() {
        @Override
        public void onReceived(ByteBuffer chunk, Transactional transactional) throws Exception {
          // No-op
        }

        @Override
        public void onFinish(HttpServiceResponder responder) throws Exception {
          responder.sendStatus(200);
        }

        @Override
        public void onError(HttpServiceResponder responder, Throwable failureCause) {
          responder.sendStatus(500);
        }
      };
    }

    @Override
    public void destroy() {
      STATES.add(ImmutablePair.of(System.identityHashCode(this), "DESTROY"));
    }
  }
}
