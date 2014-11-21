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

package co.cask.cdap.examples.helloworld;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.Service;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * This is a simple HelloWorld example that uses one stream, one dataset, one flow and one service.
 * <uL>
 *   <li>A stream to send names to.</li>
 *   <li>A flow with a single flowlet that reads the stream and stores each name in a KeyValueTable</li>
 *   <li>A service that reads the name from the KeyValueTable and responds with 'Hello [Name]!'</li>
 * </uL>
 */
public class HelloWorld extends AbstractApplication {

  @Override
  public void configure() {
    setName("HelloWorld");
    setDescription("A Hello World program for the Cask Data Application Platform");
    addStream(new Stream("who"));
    createDataset("whom", KeyValueTable.class);
    addFlow(new WhoFlow());
    addService(new Greeting());
  }

  /**
   * Sample Flow.
   */
  public static final class WhoFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with().
        setName("WhoFlow").
        setDescription("A flow that collects names").
        withFlowlets().add("saver", new NameSaver()).
        connect().fromStream("who").to("saver").
        build();
    }
  }

  /**
   * Sample Flowlet.
   */
  public static final class NameSaver extends AbstractFlowlet {

    static final byte[] NAME = { 'n', 'a', 'm', 'e' };

    @UseDataSet("whom")
    private KeyValueTable whom;

    private Metrics metrics;

    @ProcessInput
    public void process(StreamEvent event) {
      byte[] name = Bytes.toBytes(event.getBody());
      if (name.length > 0) {
        whom.write(NAME, name);

        if (name.length > 10) {
          metrics.count("names.longnames", 1);
        }
        metrics.count("names.bytes", name.length);
      }
    }
  }

  /**
   * A {@link Service} that creates a greeting using a user's name.
   */
  public static final class Greeting extends AbstractService {

    public static final String SERVICE_NAME = "Greeting";

    @Override
    protected void configure() {
      setName(SERVICE_NAME);
      setDescription("Service that creates a greeting using a user's name.");
      addHandler(new GreetingHandler());
    }
  }

  /**
   * Greeting Service handler.
   */
  public static final class GreetingHandler extends AbstractHttpServiceHandler {

    @UseDataSet("whom")
    private KeyValueTable whom;

    private Metrics metrics;

    @Path("greet")
    @GET
    public void greet(HttpServiceRequest request, HttpServiceResponder responder) {
      byte[] name = whom.read(NameSaver.NAME);
      String toGreet = name != null ? new String(name, Charsets.UTF_8) : "World";
      if (toGreet.equals("Jane Doe")) {
        metrics.count("greetings.count.jane_doe", 1);
      }
      responder.sendString(String.format("Hello %s!", toGreet));
    }
  }
}
