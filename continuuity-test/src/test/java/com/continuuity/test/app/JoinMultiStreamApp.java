package com.continuuity.test.app;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.Tick;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.google.common.base.Charsets;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class JoinMultiStreamApp implements Application {

  /**
   *
   */
  public static final class Entry {
    byte[] name;
    byte[] value;

    Entry(String name, String value) {
      this.name = name.getBytes(Charsets.UTF_8);
      this.value = value.getBytes(Charsets.UTF_8);
    }
  }

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("JoinMulti")
      .setDescription("JoinMulti")
      .withStreams().add(new Stream("s1"))
                    .add(new Stream("s2"))
                    .add(new Stream("s3"))
      .withDataSets().add(new KeyValueTable("mytable"))
      .withFlows().add(new JoinMultiFlow())
      .withProcedures().add(new Query())
      .noMapReduce()
      .noWorkflow()
      .build();
  }

  /**
   *
   */
  public static class JoinMultiFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("JoinMultiFlow")
        .setDescription("JoinMultiFlow")
        .withFlowlets()
          .add(new StreamSource("input1"))
          .add(new StreamSource("input2"))
          .add(new StreamSource("input3"))
          .add(new Terminal())
        .connect()
          .fromStream("s1").to("input1")
          .fromStream("s2").to("input2")
          .fromStream("s3").to("input3")
          .from("input1").to("Terminal")
          .from("input2").to("Terminal")
          .from("input3").to("Terminal")
        .build();
    }
  }

  /**
   *
   */
  public static class StreamSource extends AbstractFlowlet {

    private OutputEmitter<Entry> output;

    public StreamSource(String name) {
      super(name);
    }

    @ProcessInput
    public void process(StreamEvent event) {
      output.emit(new Entry(getContext().getName(), Charsets.UTF_8.decode(event.getBody()).toString()));
    }
  }

  /**
   *
   */
  public static class Terminal extends AbstractFlowlet {
    @UseDataSet("mytable")
    private KeyValueTable table;

    @ProcessInput
    public void process(Entry entry) {
      table.write(entry.name, entry.value);
    }

    @Tick(delay = 5L, unit = TimeUnit.MINUTES)
    public void tick() {
      // The tick method is to test tick doesn't affect process method trigger.
    }
  }

  /**
   *
   */
  public static class Query extends AbstractProcedure {
    @UseDataSet("mytable")
    private KeyValueTable table;

    @Handle("get")
    public void handle(ProcedureRequest request, ProcedureResponder responder) throws IOException {
      String key = request.getArgument("key");
      byte[] result = table.read(key.getBytes(Charsets.UTF_8));
      if (result == null) {
        responder.error(ProcedureResponse.Code.CLIENT_ERROR, "Key not found: " + key);
        return;
      }

      String value = new String(result, Charsets.UTF_8);

      responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), value);
    }
  }
}
