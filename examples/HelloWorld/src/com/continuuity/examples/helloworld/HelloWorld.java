/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.continuuity.examples.helloworld;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.metrics.Metrics;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;

import static com.continuuity.api.procedure.ProcedureResponse.Code.SUCCESS;

/**
 * This is a simple HelloWorld example that uses one stream, one dataset, one flow and one procedure.
 * <uL>
 *   <li>A stream to send names to.</li>
 *   <li>A flow with a single flowlet that reads the stream and stores each name in a KeyValueTable</li>
 *   <li>A procedure that reads the name from the KeyValueTable and prints Hello [Name]!</li>
 * </uL>
 */
public class HelloWorld implements Application {

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("HelloWorld")
      .setDescription("A Hello World program for the Continuuity Reactor")
      .withStreams()
        .add(new Stream("who"))
      .withDataSets()
        .add(new KeyValueTable("whom"))
      .withFlows()
        .add(new WhoFlow())
      .withProcedures()
        .add(new Greeting())
      .noMapReduce()
      .noWorkflow()
      .build();
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

