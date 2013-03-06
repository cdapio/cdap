package com.continuuity.examples.helloworld;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;

import static com.continuuity.api.procedure.ProcedureResponse.Code.SUCCESS;

/**
 * This is a simple HelloWorld example that uses one stream, on dataset, one flow and one procedure.
 * <uL>
 *   <li>A stream to send names to.</li>
 *   <li>A flow with a single flowlet that reads the stream and stores each name in a KeyValueTable</li>
 *   <li>A procedure that reads the name from the KeyValueTable and prints Hello [Name]!</li>
 * </uL>
 */
public class HelloWorld implements Application {

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with().
      setName("HelloWorld").
      setDescription("A Hello World program for the App Fabric").
      withStreams().add(new Stream("who")).
      withDataSets().add(new KeyValueTable("whom")).
      withFlows().add(new WhoFlow()).
      withProcedures().add(new Greeting()).
      build();
  }
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

  public static class NameSaver extends AbstractFlowlet {

    static final byte[] NAME = { 'n', 'a', 'm', 'e' };

    @UseDataSet("whom")
    KeyValueTable whom;

    public void processInput(StreamEvent event) throws OperationException {
      byte[] name = Bytes.toBytes(event.getBody());
      if (name != null && name.length > 0) {
        whom.write(NAME, name);
      }
    }
  }

  public static class Greeting extends AbstractProcedure {

    @UseDataSet("whom")
    KeyValueTable whom;

    @Handle("greet")
    public void greet(ProcedureRequest request, ProcedureResponder responder) throws Exception {
      byte[] name = whom.read(NameSaver.NAME);
      String toGreet = name != null ? new String(name) : "World";
      responder.sendJson(new ProcedureResponse(SUCCESS), "Hello " + toGreet + "!");
    }
  }
}

