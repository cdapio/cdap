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

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.Tick;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.FlowletException;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.service.BasicService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Flow that checks if arguments
 * are passed correctly. Only used for checking args functionality.
 */
public class ArgumentCheckApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("ArgumentCheckApp");
    setDescription("Checks if arguments are passed correctly");
    addFlow(new SimpleFlow());
    addService(new BasicService("SimpleService", new DummyHandler()));
  }

  private class SimpleFlow extends AbstractFlow {

    @Override
    protected void configureFlow() {
      setName("SimpleFlow");
      setDescription("Uses user passed value");
      addFlowlet(new SimpleGeneratorFlowlet());
      addFlowlet(new SimpleConsumerFlowlet());
      connect(new SimpleGeneratorFlowlet(), new SimpleConsumerFlowlet());
    }
  }

  private class SimpleGeneratorFlowlet extends AbstractFlowlet {
    private FlowletContext context;
    OutputEmitter<String> out;

    @Override
    public void initialize(FlowletContext context) throws FlowletException {
      this.context = context;
    }

    @Tick(delay = 1L, unit = TimeUnit.NANOSECONDS)
    public void generate() throws Exception {
      String arg = context.getRuntimeArguments().get("arg");
      if (!context.getRuntimeArguments().containsKey("arg") ||
          !context.getRuntimeArguments().get("arg").equals("test")) {
        throw new IllegalArgumentException("User runtime argument functionality not working");
      }
      out.emit(arg);
    }
  }

  private class SimpleConsumerFlowlet extends AbstractFlowlet {

    @ProcessInput
    public void process(String arg) {
      if (!arg.equals("test")) {
        throw new IllegalArgumentException("User argument from prev flowlet not passed");
      }
    }

    @ProcessInput
    public void process(int i) {
      // A dummy process method that has no matching upstream.
    }
  }

  /**
   * A handler.
   */
  public static class DummyHandler extends AbstractHttpServiceHandler {

    @Override
    public void initialize(HttpServiceContext context) throws Exception {
      if (!context.getRuntimeArguments().containsKey("arg")) {
        throw new IllegalArgumentException("User runtime argument functionality not working.");
      }
    }

    @GET
    @Path("ping")
    public void handle(HttpServiceRequest request, HttpServiceResponder responder) throws IOException {
      responder.sendStatus(200);
    }
  }

}
