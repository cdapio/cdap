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

import co.cask.cdap.api.annotation.Handle;
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
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.procedure.ProcedureResponse;

import static co.cask.cdap.api.procedure.ProcedureResponse.Code.SUCCESS;

/**
 * This is a simple HelloWorld example that uses one stream, one dataset, one flow and one procedure.
 * <uL>
 *   <li>A stream to send names to.</li>
 *   <li>A flow with a single flowlet that reads the stream and stores each name in a KeyValueTable</li>
 *   <li>A procedure that reads the name from the KeyValueTable and prints 'Hello [Name]!'</li>
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
    addProcedure(new Greeting());
  }

  /**
   * Sample Flow.
   */
  public static class WhoFlow implements Flow {

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
  public static class NameSaver extends AbstractFlowlet {

    static final byte[] NAME = { 'n', 'a', 'm', 'e' };

    @UseDataSet("whom")
    KeyValueTable whom;
    Metrics flowletMetrics;

    @ProcessInput
    public void process(StreamEvent event) {
      byte[] name = Bytes.toBytes(event.getBody());
      if (name != null && name.length > 0) {
        whom.write(NAME, name);
      }
      if (name.length > 10) {
        flowletMetrics.count("names.longnames", 1);
      }
      flowletMetrics.count("names.bytes", name.length);
    }
  }

  /**
   * Sample Procedure.
   */
  public static class Greeting extends AbstractProcedure {

    @UseDataSet("whom")
    KeyValueTable whom;
    Metrics procedureMetrics;

    @Handle("greet")
    public void greet(ProcedureRequest request, ProcedureResponder responder) throws Exception {
      byte[] name = whom.read(NameSaver.NAME);
      String toGreet = name != null ? new String(name) : "World";
      if (toGreet.equals("Jane Doe")) {
        procedureMetrics.count("greetings.count.jane_doe", 1);
      }
      responder.sendJson(new ProcedureResponse(SUCCESS), "Hello " + toGreet + "!");
    }
  }
}

