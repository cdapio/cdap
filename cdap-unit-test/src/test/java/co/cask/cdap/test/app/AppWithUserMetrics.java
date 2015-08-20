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

package co.cask.cdap.test.app;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.metrics.collect.MetricsEmitter;

/**
 *
 */
public class AppWithUserMetrics extends AbstractApplication {

  @Override
  public void configure() {
    setName("fooApp");
    addStream(new Stream("fooStream"));
    addFlow(new FooFlow());
  }

  /**
   * Sample Flow.
   */
  public static final class FooFlow extends AbstractFlow {

    @Override
    protected void configureFlow() {
      setName("fooFlow");
      addFlowlet("fooFlowlet", new FooFlowlet());
      connectStream("fooStream", "fooFlowlet");
    }
  }

  /**
   * Sample Flowlet.
   */
  public static final class FooFlowlet extends AbstractFlowlet {

    private Metrics metrics;

    @ProcessInput
    public void process(StreamEvent event) {
      metrics.count("my.events.count", 1);
      Integer index = Integer.valueOf(event.getHeaders().get("index"));
      if (index % 2 == 1) {
        metrics.count("my.events.odd.count", 1);
        metrics.gauge("my.events.odd.last", index);
      }
    }
  }
}
