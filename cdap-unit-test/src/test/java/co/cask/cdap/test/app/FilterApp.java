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

package co.cask.cdap.test.app;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.service.BasicService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

import java.io.IOException;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * App that filters based on a threshold.
 * To test runtimeArgs.
 */
public class FilterApp extends AbstractApplication {
  private static final byte[] highPass = Bytes.toBytes("h");

  @Override
  public void configure() {
    setName("FilterApp");
    setDescription("Application for filtering numbers. Test runtimeargs.");
    addStream(new Stream("input"));
    createDataset("count", KeyValueTable.class);
    addFlow(new FilterFlow());
    addService(new BasicService("CountService", new CountHandler()));
  }

  /**
   * Flow to implement highpass filter.
   */
  public static class FilterFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("FilterFlow")
        .setDescription("Flow for counting words")
        .withFlowlets()
        .add("filter", new Filter())
        .connect().fromStream("input").to("filter")
        .build();
    }
  }

  /**
   * Flowlet that has filtering logic.
   */
  public static class Filter extends AbstractFlowlet {

    @UseDataSet("count")
    private KeyValueTable counters;
    private long threshold = 0L;

    @ProcessInput
    public void process (StreamEvent event) {
      //highpass filter.
      String value = Bytes.toString(event.getBody());
      if (Long.parseLong(value) > threshold) {
        counters.increment(highPass, 1L);
      }
    }

    @Override
    public void initialize(FlowletContext context) throws Exception {
      Map<String, String> args = context.getRuntimeArguments();
      if (args.containsKey("threshold")) {
        this.threshold = Long.parseLong(args.get("threshold"));
      }
      super.initialize(context);
    }
  }


  /**
   * return counts.
   */
  public static class CountHandler extends AbstractHttpServiceHandler {
    @UseDataSet("count")
    private KeyValueTable counters;

    @GET
    @Path("result")
    public void handle(HttpServiceRequest request, HttpServiceResponder responder) throws IOException {
      byte[] result = counters.read(highPass);
      if (result == null) {
        responder.sendStatus(404);
      } else {
        responder.sendJson(Bytes.toLong(result));
      }
    }
  }

}
