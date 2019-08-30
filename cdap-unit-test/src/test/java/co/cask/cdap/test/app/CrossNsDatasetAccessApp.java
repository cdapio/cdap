/*
 * Copyright © 2016 Cask Data, Inc.
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
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.AbstractFlow;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.StreamEvent;

/**
 * An app using dataset from another namespace
 */
public class CrossNsDatasetAccessApp extends AbstractApplication {
  public static final String APP_NAME = "WriterApp";
  public static final String STREAM_NAME = "dataStream";
  public static final String FLOW_NAME = "dataFlow";
  public static final String OUTPUT_DATASET_NS = "output.dataset.ns";
  public static final String OUTPUT_DATASET_NAME = "output.dataset.name";

  @Override
  public void configure() {
    setName(APP_NAME);
    setDescription("Writes from a stream to DS in another NS");
    addStream(new Stream(STREAM_NAME));
    addFlow(new WhoFlow());
  }

  /**
   * Sample Flow.
   */
  public static final class WhoFlow extends AbstractFlow {
    @Override
    protected void configure() {
      setName(FLOW_NAME);
      setDescription("A flow that collects names");
      addFlowlet("saver", new NameSaver());
      connectStream("dataStream", "saver");
    }
  }

  /**
   * Sample Flowlet.
   */
  public static final class NameSaver extends AbstractFlowlet {
    private KeyValueTable whom;

    @ProcessInput
    public void process(StreamEvent event) {
      byte[] name = Bytes.toBytes(event.getBody());
      if (name.length > 0) {
        whom.write(name, name);
      }
    }

    @Override
    public void initialize(FlowletContext context) throws Exception {
      super.initialize(context);
      whom = context.getDataset(context.getRuntimeArguments().get(OUTPUT_DATASET_NS),
                                context.getRuntimeArguments().get(OUTPUT_DATASET_NAME));
    }
  }
}
