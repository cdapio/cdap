/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.Tick;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.service.BasicService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 *
 */
public class JoinMultiStreamApp extends AbstractApplication {

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
  public void configure() {
    setName("JoinMulti");
    setDescription("JoinMulti");
    addStream(new Stream("s1"));
    addStream(new Stream("s2"));
    addStream(new Stream("s3"));
    createDataset("mytable", KeyValueTable.class);
    addFlow(new JoinMultiFlow());
    addService(new BasicService("QueryService", new QueryHandler()));
  }

  /**
   *
   */
  public static class JoinMultiFlow extends AbstractFlow {

    @Override
    protected void configure() {
      setName("JoinMultiFlow");
      setDescription("JoinMultiFlow");
      addFlowlet(new StreamSource("input1"));
      addFlowlet(new StreamSource("input2"));
      addFlowlet(new StreamSource("input3"));
      addFlowlet(new Terminal());
      connectStream("s1", "input1");
      connectStream("s2", "input2");
      connectStream("s3", "input3");
      connect("input1", "Terminal");
      connect("input2", "Terminal");
      connect("input3", "Terminal");
    }
  }

  /**
   *
   */
  public static class StreamSource extends AbstractFlowlet {

    private final String name;
    private OutputEmitter<Entry> output;

    public StreamSource(String name) {
      this.name = name;
    }

    @ProcessInput
    public void process(StreamEvent event) {
      output.emit(new Entry(getContext().getName(), Charsets.UTF_8.decode(event.getBody()).toString()));
    }

    @Override
    protected void configure() {
      setName(name);
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
  public static class QueryHandler extends AbstractHttpServiceHandler {
    @UseDataSet("mytable")
    private KeyValueTable table;

    @GET
    @Path("{key}")
    public void handle(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("key") String key) throws IOException {
      byte[] result = table.read(key.getBytes(Charsets.UTF_8));
      if (result == null) {
        responder.sendError(404, "Key not found: " + key);
        return;
      }
      String value = new String(result, Charsets.UTF_8);
      responder.sendJson(value);
    }
  }
}
