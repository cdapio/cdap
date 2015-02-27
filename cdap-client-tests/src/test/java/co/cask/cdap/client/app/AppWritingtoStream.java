/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.client.app;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.Tick;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.data.stream.StreamBatchWriter;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class AppWritingtoStream extends AbstractApplication {

  public static final String APPNAME = "appName";
  public static final String STREAM = "myStream";
  public static final String FLOW1 = "flow1";
  public static final String FLOW2 = "flow2";

  @Override
  public void configure() {
    setName(APPNAME);
    addStream(new Stream(STREAM));
    addFlow(new MyFlow());
    addFlow(new SimpleFlow());
  }

  private static final class MyFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with().setName(FLOW1).setDescription("Blah Blah").withFlowlets()
        .add("source", new SourceFlowlet())
        .add("sink", new SinkFlowlet()).connect().from("source").to("sink").build();
    }
  }

  private static final class SourceFlowlet extends AbstractFlowlet {

    private static OutputEmitter<String> data;

    @Tick(delay = 1, unit = TimeUnit.SECONDS)
    public void generate() {
      data.emit("hello");
    }
  }

  private static final class SinkFlowlet extends AbstractFlowlet {

    private static final Logger LOG = LoggerFactory.getLogger(SinkFlowlet.class);

    @ProcessInput
    public void receive(String data) throws Exception {
      StreamBatchWriter writer = getContext().writeInBatch(STREAM, "text/data");
      writer.write(ByteBuffer.wrap(Bytes.toBytes(data + "\n")));
      writer.write(ByteBuffer.wrap(Bytes.toBytes(data + "\n")));
      writer.close();
//      getContext().write(STREAM, data);
    }
  }

  private static final class SimpleFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with().setName(FLOW2).setDescription("Blah ").withFlowlets()
        .add("flowlet", new StreamFlowlet())
        .connect()
        .fromStream(STREAM)
        .to("flowlet").build();
    }
  }

  private static final class StreamFlowlet extends AbstractFlowlet {

    private static final Logger LOG = LoggerFactory.getLogger(StreamFlowlet.class);

    @ProcessInput
    public void receive(StreamEvent data) {
      LOG.error("Received from Stream : {}", Bytes.toString(data.getBody()));
    }
  }
}
