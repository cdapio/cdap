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

package co.cask.cdap.stream.app;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public final class StreamApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("StreamApp");
    setDescription("StreamApp");
    addStream(new Stream("stream"));
    createDataset("streamout", KeyValueTable.class);
    addFlow(new StreamFlow());
  }

  /**
   *
   */
  public static final class StreamFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("StreamFlow")
        .setDescription("StreamFlow")
        .withFlowlets()
          .add("reader", new StreamReader())
        .connect()
          .fromStream("stream").to("reader")
        .build();
    }
  }

  /**
   *
   */
  public static final class StreamReader extends AbstractFlowlet {

    private static final Logger LOG = LoggerFactory.getLogger(StreamReader.class);

    @UseDataSet("streamout")
    KeyValueTable keyValueTable;

    @ProcessInput
    public void process(StreamEvent event) throws InterruptedException {
      String msg = Charsets.UTF_8.decode(event.getBody()).toString();
      LOG.info(msg);
      keyValueTable.increment(msg.getBytes(Charsets.UTF_8), 1L);
    }
  }
}
