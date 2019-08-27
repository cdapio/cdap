/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FailurePolicy;
import co.cask.cdap.api.flow.flowlet.FlowletConfigurer;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;

import java.util.Map;

/**
 * App that filters based on a threshold.
 * To test runtimeArgs.
 */
public class FilterAppWithNewFlowAPI extends AbstractApplication {
  static final byte[] HIGH_PASS = Bytes.toBytes("h");

  @Override
  public void configure() {
    setName("FilterApp");
    setDescription("Application for filtering numbers. Test runtimeargs.");
    addFlow(new FilterFlow());
  }

  /**
   * Flow to implement highpass filter.
   */
  public static class FilterFlow extends AbstractFlow {

    @Override
    public void configure() {
      setName("FilterFlow");
      setDescription("Flow for counting words");
      addStream("input");
      addFlowlet("pass", new PassFlowlet());
      addFlowlet("filter", new Filter());
      connectStream("input", "pass");
      connect("pass", "filter");
    }
  }

  /**
   * Flowlet that simply passes event to the next flowlet.
   */
  public static class PassFlowlet extends AbstractFlowlet {

    private OutputEmitter<StreamEvent> emitter;

    @Override
    public void configure() {
      setName("pass");
      setDescription("NoOp Flowlet that passes the event to the next flowlet");
      setFailurePolicy(FailurePolicy.IGNORE);
      createDataset("count", KeyValueTable.class);
    }

    @ProcessInput
    public void process(StreamEvent event) {
      emitter.emit(event);
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
        counters.increment(HIGH_PASS, 1L);
      }
    }

    @Override
    public void configure(FlowletConfigurer configurer) {
      super.configure(configurer);
      setName("filter");
      setDescription("Trial Description");
      setFailurePolicy(FailurePolicy.RETRY);
      setResources(new Resources());
      setProperties(ImmutableMap.of("key", "value"));
    }

    @Override
    public void initialize(FlowletContext context) throws Exception {
      super.initialize(context);
      Map<String, String> args = context.getRuntimeArguments();
      if (args.containsKey("threshold")) {
        this.threshold = Long.parseLong(args.get("threshold"));
      }
      Assert.assertEquals("value", context.getSpecification().getProperties().get("key"));
    }
  }
}

