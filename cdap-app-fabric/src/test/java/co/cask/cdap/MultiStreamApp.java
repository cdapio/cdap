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
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * This is a sample word count app that is used in testing in
 * many places.
 */
public class MultiStreamApp extends AbstractApplication {

  private static final Logger LOG = LoggerFactory.getLogger(MultiStreamApp.class);

  @Override
  public void configure() {
    setName("MultiStreamApp");
    setDescription("Application for testing changing stream-flowlet connections");
    addStream(new Stream("stream1"));
    addStream(new Stream("stream2"));
    addStream(new Stream("stream3"));
    addStream(new Stream("stream4"));
    createDataset("table", Table.class);
    addFlow(new CounterFlow());
    addService(new CountersService());
  }

  /**
   *
   */
  public static class CounterFlow extends AbstractFlow {

    @Override
    protected void configureFlow() {
      setName("CounterFlow");
      setDescription("Flow for counting events");
      addFlowlet("counter1", new EventsCounter1());
      addFlowlet("counter2", new EventsCounter2());
      connectStream("stream1", "counter1");
      connectStream("stream3", "counter2");
    }
  }

  /**
   *
   */
  public static class EventsCounter1 extends AbstractFlowlet {
    @UseDataSet("table")
    private Table table;

    @ProcessInput()
    public void process(StreamEvent event) {
      table.increment(new Increment("row", "counter1", 1));
    }
  }

  /**
   *
   */
  public static class EventsCounter2 extends AbstractFlowlet {
    @UseDataSet("table")
    private Table table;

    @ProcessInput("stream3")
    public void process(StreamEvent event) {
      table.increment(new Increment("row", "counter2", 1));
    }
  }

  public static class CountersService extends AbstractService {
    @Override
    protected void configure() {
      addHandler(new CountersHandler());
    }
  }

  public static class CountersHandler extends AbstractHttpServiceHandler {
    @UseDataSet("table")
    private Table table;

    @GET
    @Path("counter")
    public void handle(HttpServiceRequest request, HttpServiceResponder responder) throws IOException {
      responder.sendJson(table.get(new Get("row")).getLong("column"));
    }

  }
}
